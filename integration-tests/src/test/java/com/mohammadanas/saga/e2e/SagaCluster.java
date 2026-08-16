package com.mohammadanas.saga.e2e;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.mohammadanas.saga.inventory.InventoryServiceApplication;
import com.mohammadanas.saga.notification.NotificationServiceApplication;
import com.mohammadanas.saga.notification.messaging.NotificationTopics;
import com.mohammadanas.saga.notification.service.LoggingNotificationSender;
import com.mohammadanas.saga.order.OrderServiceApplication;
import com.mohammadanas.saga.orchestrator.SagaOrchestratorApplication;
import com.mohammadanas.saga.orchestrator.messaging.SagaTopics;
import com.mohammadanas.saga.payment.PaymentServiceApplication;
import com.mohammadanas.saga.scheduler.SchedulerServiceApplication;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.LoggerFactory;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.context.WebServerApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Boots the whole system: one Kafka, one Postgres, one Redis, and all six services.
 *
 * <h2>What is real</h2>
 *
 * <p>Everything that carries a message or a row. Real Kafka topics and consumer groups, real
 * Postgres databases (one per service, as §7 requires), real Redis for the scheduler's lock,
 * real HTTP for order-service's REST API and the scheduler's poll of the orchestrator, and
 * every service's actual production code. Nothing is stubbed, and no service is replaced by
 * a test double. The single substitution in the whole suite is {@link MutableClock}, and it
 * replaces a clock rather than a collaborator.
 *
 * <h2>What is not real, stated plainly</h2>
 *
 * <p>The six services run as six Spring contexts inside <strong>one JVM</strong>, not as six
 * operating-system processes. This suite therefore does not exercise process isolation,
 * independent deployment, or a service dying mid-saga — it exercises the message flows,
 * the persistence, and the state machine. That is where this project's correctness lives,
 * and it is what §8.3's failure cases are about.
 *
 * <p>One consequence is worth knowing about when reading this class: because all six modules
 * sit on one classpath, every context sees every auto-configuration. inventory-service is not
 * a web application, but {@code spring-boot-starter-web} is present because order-service
 * needs it — so the web type is forced off explicitly here rather than being inferred as it
 * is in production. The same applies to Quartz and JPA on services that do not use them.
 * These are stated as properties below rather than left implicit.
 *
 * <h2>Why the services' own application.yml files are not used</h2>
 *
 * <p>They cannot be. Six modules on one classpath means six files at {@code /application.yml}
 * and Spring loads exactly one of them, silently. Config-file loading is therefore switched
 * off and every property this suite depends on is stated explicitly below. The settings that
 * genuinely matter — the JSON serializer, type headers being off, {@code earliest} offsets,
 * per-service consumer groups — are the ones a mistake would break loudly, because messages
 * simply would not arrive.
 */
public final class SagaCluster {

    private static final DockerImageName POSTGRES_IMAGE = DockerImageName.parse("postgres:16-alpine");
    private static final DockerImageName KAFKA_IMAGE = DockerImageName.parse("apache/kafka:3.8.0");
    private static final DockerImageName REDIS_IMAGE = DockerImageName.parse("redis:7-alpine");

    private static final List<String> DATABASES =
            List.of("order_db", "inventory_db", "payment_db", "notification_db", "orchestrator_db");

    private static PostgreSQLContainer<?> postgres;
    private static KafkaContainer kafka;
    private static GenericContainer<?> redis;

    private static ConfigurableApplicationContext orderService;
    private static ConfigurableApplicationContext inventoryService;
    private static ConfigurableApplicationContext paymentService;
    private static ConfigurableApplicationContext notificationService;
    private static ConfigurableApplicationContext orchestratorService;
    private static ConfigurableApplicationContext schedulerService;

    private static final MutableClock CLOCK = new MutableClock();
    private static ListAppender<ILoggingEvent> notificationLog;

    private static boolean started;

    private SagaCluster() {
    }

    public static MutableClock clock() {
        return CLOCK;
    }

    /** Idempotent: the cluster is a JVM-wide singleton, started once however many classes use it. */
    public static synchronized void start() {
        if (started) {
            return;
        }

        // Everything from here owns a real resource, and a failure part-way through would
        // otherwise leave containers and Spring contexts running with nothing holding a
        // reference to them: `started` would still be false, so stop() would decline to do
        // anything, and @AfterAll would clean up nothing. Testcontainers' Ryuk would
        // eventually reap the containers, but the JVM would sit there with half a cluster
        // in it for the rest of the run.
        try {
            startInfrastructure();
            createTopics();
            createDatabases();

            orderService = boot(OrderServiceApplication.class, "order-service", orderProperties(), true);
            inventoryService = boot(InventoryServiceApplication.class, "inventory-service",
                    inventoryProperties(), false);
            paymentService = boot(PaymentServiceApplication.class, "payment-service", paymentProperties(), false);
            notificationService = boot(NotificationServiceApplication.class, "notification-service",
                    notificationProperties(), false);

            // The orchestrator takes an extra source so its Clock can be the test's.
            orchestratorService = new SpringApplicationBuilder(
                    SagaOrchestratorApplication.class, ControllableClockConfig.class)
                    .web(WebApplicationType.SERVLET)
                    .properties(orchestratorProperties())
                    .run();

            // Scheduler last: it needs the orchestrator's actual port.
            schedulerService = boot(SchedulerServiceApplication.class, "scheduler-service",
                    schedulerProperties(port(orchestratorService)), false);

            // Only now, once every context is up. Each Spring Boot startup re-initialises the
            // Logback context, which resets its loggers and discards any appender attached
            // beforehand — so capturing earlier silently captures nothing, and the assertions
            // fail against an empty list while the messages are plainly there in the output.
            captureNotificationLog();
        } catch (RuntimeException | Error e) {
            // Tear down whatever did come up, then let the original failure surface — the
            // reason the cluster would not start is far more useful than anything thrown
            // while cleaning up after it.
            started = true;
            try {
                stop();
            } catch (RuntimeException | Error cleanupFailure) {
                e.addSuppressed(cleanupFailure);
            }
            throw e;
        }

        started = true;
        Runtime.getRuntime().addShutdownHook(new Thread(SagaCluster::stop));
    }

    private static void startInfrastructure() {
        postgres = new PostgreSQLContainer<>(POSTGRES_IMAGE);
        kafka = new KafkaContainer(KAFKA_IMAGE);

        // Assigned from the constructor first, then configured. Written as
        // `new GenericContainer<>(...).withExposedPorts(...)` the constructor's result — an
        // AutoCloseable — is handed to a method before any field holds it, so nothing can
        // see that it is owned. Unlike the @Container fields elsewhere, these containers
        // have no framework managing them: stop() is the only thing that closes them, which
        // makes clear ownership from the first statement worth having.
        redis = new GenericContainer<>(REDIS_IMAGE);
        redis.addExposedPort(6379);

        postgres.start();
        kafka.start();
        redis.start();
    }

    /**
     * Creates every topic up front, before any service starts.
     *
     * <p>Necessary, and for a reason worth spelling out because it cost a debugging cycle.
     * Each service declares {@code NewTopic} beans only for the topics it <em>owns</em>, so
     * whichever service starts first inevitably subscribes to topics that do not exist yet
     * — inventory-service subscribes to {@code inventory.commands.reserve-inventory.v1},
     * which saga-orchestrator creates, and the orchestrator subscribes to event topics the
     * executors create. There is no start order that avoids this, because the dependency is
     * a cycle.
     *
     * <p>A consumer subscribed to a missing topic does not fail; it logs
     * {@code UNKNOWN_TOPIC_OR_PARTITION} and waits for its next metadata refresh, which by
     * default is {@code metadata.max.age.ms} — <strong>five minutes</strong>. The service
     * looks healthy and silently consumes nothing for far longer than any sensible test
     * will wait, which is exactly how this first presented: sagas sitting in
     * {@code AWAITING_INVENTORY} until the sweep compensated them.
     *
     * <p>Doing it here also matches what §5.3's own note says happens in production —
     * "topic creation would be an operations concern rather than an application one". The
     * services' {@code NewTopic} beans are a development convenience, not the mechanism
     * this suite should depend on.
     */
    private static void createTopics() {
        List<String> topics = List.of(
                SagaTopics.ORDER_CREATED,
                SagaTopics.INVENTORY_RESERVED,
                SagaTopics.INVENTORY_RESERVATION_FAILED,
                SagaTopics.INVENTORY_RELEASED,
                SagaTopics.PAYMENT_COMPLETED,
                SagaTopics.PAYMENT_FAILED,
                SagaTopics.PAYMENT_REFUNDED,
                SagaTopics.RESERVE_INVENTORY,
                SagaTopics.RELEASE_INVENTORY,
                SagaTopics.PROCESS_PAYMENT,
                SagaTopics.REFUND_PAYMENT,
                SagaTopics.CONFIRM_ORDER,
                SagaTopics.CANCEL_ORDER,
                NotificationTopics.ORDER_CONFIRMED,
                NotificationTopics.ORDER_CANCELLED);

        try (AdminClient admin = AdminClient.create(
                Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {

            admin.createTopics(topics.stream()
                            .map(name -> new NewTopic(name, 3, (short) 1))
                            .toList())
                    .all()
                    .get(60, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("Could not pre-create Kafka topics", e);
        }
    }

    /**
     * One database per service. Not cosmetic: notification-service and saga-orchestrator both
     * own a table called {@code processed_messages}, and inventory-service and payment-service
     * both own {@code processed_commands}. Sharing a database would have them silently
     * scribbling over each other's dedup records — and §7 says each service owns its schema
     * anyway.
     */
    private static void createDatabases() {
        try (Connection connection = DriverManager.getConnection(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
             Statement statement = connection.createStatement()) {

            for (String database : DATABASES) {
                statement.execute("CREATE DATABASE " + database);
            }
        } catch (Exception e) {
            throw new IllegalStateException("Could not create per-service databases", e);
        }
    }

    private static void captureNotificationLog() {
        notificationLog = new ListAppender<>();
        notificationLog.start();
        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(LoggingNotificationSender.class))
                .addAppender(notificationLog);
    }

    private static ConfigurableApplicationContext boot(
            Class<?> application, String name, Map<String, Object> properties, boolean web) {
        return new SpringApplicationBuilder(application)
                .web(web ? WebApplicationType.SERVLET : WebApplicationType.NONE)
                .properties(properties)
                .run();
    }

    // ---------------------------------------------------------------- properties

    private static Map<String, Object> base(String consumerGroup, String database) {
        Map<String, Object> properties = new LinkedHashMap<>();

        // No application.yml anywhere — see the class comment.
        properties.put("spring.config.location", "optional:classpath:/no-config/");
        properties.put("spring.main.web-application-type", "none");

        if (database != null) {
            properties.put("spring.datasource.url", jdbcUrl(database));
            properties.put("spring.datasource.username", postgres.getUsername());
            properties.put("spring.datasource.password", postgres.getPassword());
            properties.put("spring.jpa.hibernate.ddl-auto", "update");
            properties.put("spring.jpa.open-in-view", false);
        }

        properties.put("spring.kafka.bootstrap-servers", kafka.getBootstrapServers());

        // Mirrors every service's real producer config. Type headers stay off: the contract
        // is the topic plus the consumer's declared record (§5.3).
        properties.put("spring.kafka.producer.key-serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("spring.kafka.producer.value-serializer",
                "org.springframework.kafka.support.serializer.JsonSerializer");
        properties.put("spring.kafka.producer.properties.spring.json.add.type.headers", false);
        properties.put("spring.kafka.producer.acks", "all");

        properties.put("spring.kafka.consumer.group-id", consumerGroup);
        properties.put("spring.kafka.consumer.auto-offset-reset", "earliest");
        properties.put("spring.kafka.consumer.key-deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("spring.kafka.consumer.value-deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");

        // Quartz belongs to scheduler-service alone; it is only visible here because the
        // classpath is shared. Left on, it would start an empty scheduler in every context.
        properties.put("spring.autoconfigure.exclude",
                "org.springframework.boot.autoconfigure.quartz.QuartzAutoConfiguration");

        return properties;
    }

    private static Map<String, Object> orderProperties() {
        Map<String, Object> properties = base("order-service", "order_db");
        properties.put("spring.main.web-application-type", "servlet");
        properties.put("server.port", 0);
        return properties;
    }

    private static Map<String, Object> inventoryProperties() {
        return base("inventory-service", "inventory_db");
    }

    private static Map<String, Object> paymentProperties() {
        Map<String, Object> properties = base("payment-service", "payment_db");
        // The documented default, restated so the failure-path test's arithmetic is
        // readable next to it rather than inherited from a yml this suite does not load.
        properties.put("payment.simulation.failure-threshold", "1000.00");
        return properties;
    }

    private static Map<String, Object> notificationProperties() {
        return base("notification-service", "notification_db");
    }

    private static Map<String, Object> orchestratorProperties() {
        Map<String, Object> properties = base("saga-orchestrator", "orchestrator_db");
        properties.put("spring.main.web-application-type", "servlet");
        properties.put("server.port", 0);
        // Left at a realistic value. The timeout test moves the clock instead of shortening
        // this, so no other test has to race it.
        properties.put("saga.timeout", "PT5M");
        return properties;
    }

    private static Map<String, Object> schedulerProperties(int orchestratorPort) {
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("spring.config.location", "optional:classpath:/no-config/");
        properties.put("spring.main.web-application-type", "none");

        // scheduler-service holds no state of its own (§4), but JPA is on the shared
        // classpath and would otherwise demand a datasource it has no use for.
        properties.put("spring.autoconfigure.exclude",
                "org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration,"
                        + "org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration");

        properties.put("spring.data.redis.host", redis.getHost());
        properties.put("spring.data.redis.port", redis.getMappedPort(6379));

        properties.put("spring.quartz.job-store-type", "memory");
        properties.put("scheduler.interval", "PT1S");
        properties.put("scheduler.lock-ttl", "PT30S");
        properties.put("scheduler.orchestrator-base-url", "http://localhost:" + orchestratorPort);
        return properties;
    }

    private static String jdbcUrl(String database) {
        return "jdbc:postgresql://" + postgres.getHost() + ":" + postgres.getMappedPort(5432) + "/" + database;
    }

    // ---------------------------------------------------------------- accessors

    public static String orderServiceBaseUrl() {
        return "http://localhost:" + port(orderService);
    }

    private static int port(ConfigurableApplicationContext context) {
        return ((WebServerApplicationContext) context).getWebServer().getPort();
    }

    public static <T> T orderBean(Class<T> type) {
        return orderService.getBean(type);
    }

    public static <T> T inventoryBean(Class<T> type) {
        return inventoryService.getBean(type);
    }

    public static <T> T paymentBean(Class<T> type) {
        return paymentService.getBean(type);
    }

    public static <T> T notificationBean(Class<T> type) {
        return notificationService.getBean(type);
    }

    public static <T> T orchestratorBean(Class<T> type) {
        return orchestratorService.getBean(type);
    }

    /** Every line {@code LoggingNotificationSender} has emitted — the customer-facing output. */
    public static List<String> notificationMessages() {
        List<String> messages = new ArrayList<>();
        // Copied under the appender's lock: Kafka listener threads append concurrently.
        synchronized (notificationLog) {
            notificationLog.list.forEach(event -> messages.add(event.getFormattedMessage()));
        }
        return messages;
    }

    /**
     * Silences inventory-service's listeners, so commands addressed to it pile up unconsumed.
     *
     * <p>This is how the timeout test creates a genuinely stalled saga: not by mocking a
     * failure, but by making a real service stop answering, which is exactly the situation
     * §4 describes ("a consumer is down"). The messages stay on the topic and are processed
     * for real once it is switched back on.
     */
    public static void pauseInventoryListeners() {
        inventoryBean(KafkaListenerEndpointRegistry.class).getListenerContainers()
                .forEach(container -> container.stop());
    }

    public static void resumeInventoryListeners() {
        inventoryBean(KafkaListenerEndpointRegistry.class).getListenerContainers()
                .forEach(container -> container.start());
    }

    public static synchronized void stop() {
        if (!started) {
            return;
        }
        started = false;

        closeQuietly(schedulerService);
        closeQuietly(orchestratorService);
        closeQuietly(notificationService);
        closeQuietly(paymentService);
        closeQuietly(inventoryService);
        closeQuietly(orderService);

        stopQuietly(redis);
        stopQuietly(kafka);
        stopQuietly(postgres);
    }

    private static void closeQuietly(ConfigurableApplicationContext context) {
        if (context != null) {
            try {
                context.close();
            } catch (RuntimeException ignored) {
                // Shutting down a test fixture; a noisy close must not mask a test result.
            }
        }
    }

    private static void stopQuietly(GenericContainer<?> container) {
        if (container != null) {
            try {
                container.stop();
            } catch (RuntimeException ignored) {
                // As above.
            }
        }
    }
}
