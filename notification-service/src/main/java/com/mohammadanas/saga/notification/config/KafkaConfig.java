package com.mohammadanas.saga.notification.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;

@Configuration
public class KafkaConfig {

    /**
     * Resolves inbound JSON against the {@code @KafkaListener} method's parameter type, so
     * the contract is the topic plus the declared record rather than a producer-supplied
     * class name.
     *
     * <p>No {@code NewTopic} beans: notification-service owns no topics. It consumes two
     * that order-service is expected to own.
     */
    @Bean
    public RecordMessageConverter jsonMessageConverter() {
        return new StringJsonMessageConverter();
    }
}
