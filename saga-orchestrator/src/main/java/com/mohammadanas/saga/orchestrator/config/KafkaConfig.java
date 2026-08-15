package com.mohammadanas.saga.orchestrator.config;

import com.mohammadanas.saga.orchestrator.messaging.SagaTopics;
import java.time.Clock;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;

@Configuration
public class KafkaConfig {

    @Bean
    public RecordMessageConverter jsonMessageConverter() {
        return new StringJsonMessageConverter();
    }

    /**
     * Injected rather than calling {@code Instant.now()} inline so timeout deadlines and
     * the stuck-saga sweep can be tested against a controlled clock instead of a sleep
     * (§8.3 case 3).
     */
    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }

    // The orchestrator owns every command topic: it is their only publisher.
    @Bean
    public NewTopic reserveInventoryTopic() {
        return TopicBuilder.name(SagaTopics.RESERVE_INVENTORY).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic releaseInventoryTopic() {
        return TopicBuilder.name(SagaTopics.RELEASE_INVENTORY).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic processPaymentTopic() {
        return TopicBuilder.name(SagaTopics.PROCESS_PAYMENT).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic refundPaymentTopic() {
        return TopicBuilder.name(SagaTopics.REFUND_PAYMENT).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic confirmOrderTopic() {
        return TopicBuilder.name(SagaTopics.CONFIRM_ORDER).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic cancelOrderTopic() {
        return TopicBuilder.name(SagaTopics.CANCEL_ORDER).partitions(3).replicas(1).build();
    }

}
