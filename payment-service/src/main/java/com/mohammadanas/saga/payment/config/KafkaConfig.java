package com.mohammadanas.saga.payment.config;

import com.mohammadanas.saga.payment.messaging.PaymentTopics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;

@Configuration
public class KafkaConfig {

    /**
     * Resolves inbound JSON against the {@code @KafkaListener} method's parameter type,
     * so the contract is the topic plus the declared record rather than a
     * producer-supplied class name.
     */
    @Bean
    public RecordMessageConverter jsonMessageConverter() {
        return new StringJsonMessageConverter();
    }

    @Bean
    public NewTopic paymentCompletedTopic() {
        return TopicBuilder.name(PaymentTopics.PAYMENT_COMPLETED).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic paymentFailedTopic() {
        return TopicBuilder.name(PaymentTopics.PAYMENT_FAILED).partitions(3).replicas(1).build();
    }
}
