package com.mohammadanas.saga.order.config;

import com.mohammadanas.saga.order.messaging.OrderTopics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;

@Configuration
public class KafkaConfig {

    /**
     * Resolves inbound JSON against the {@code @KafkaListener} method's parameter type.
     *
     * <p>Chosen over {@code JsonDeserializer} type headers so that the contract is the
     * topic plus the declared record type, not a producer-supplied class name. That keeps
     * order-service decoupled from the orchestrator's package layout and avoids the
     * trusted-packages configuration that type headers otherwise require.
     */
    @Bean
    public RecordMessageConverter jsonMessageConverter() {
        return new StringJsonMessageConverter();
    }

    /**
     * order-service owns this topic, so it declares it. Command topics it merely consumes
     * are declared by their owner; in a real deployment topic creation would be an
     * operations concern rather than an application one.
     */
    @Bean
    public NewTopic orderCreatedTopic() {
        return TopicBuilder.name(OrderTopics.ORDER_CREATED).partitions(3).replicas(1).build();
    }

    /**
     * The two terminal-state topics are order-service's as well (ARCHITECTURE.md section
     * 5.3), so it declares them too. notification-service consumes both and owns neither,
     * which is why it has no {@code NewTopic} beans of its own — without these, its
     * listeners would sit on topics nothing had created.
     */
    @Bean
    public NewTopic orderConfirmedTopic() {
        return TopicBuilder.name(OrderTopics.ORDER_CONFIRMED).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic orderCancelledTopic() {
        return TopicBuilder.name(OrderTopics.ORDER_CANCELLED).partitions(3).replicas(1).build();
    }
}
