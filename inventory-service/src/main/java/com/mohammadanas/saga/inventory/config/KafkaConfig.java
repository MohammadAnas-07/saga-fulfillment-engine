package com.mohammadanas.saga.inventory.config;

import com.mohammadanas.saga.inventory.messaging.InventoryTopics;
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
     * producer-supplied class name. Keeps this service decoupled from the orchestrator's
     * package layout.
     */
    @Bean
    public RecordMessageConverter jsonMessageConverter() {
        return new StringJsonMessageConverter();
    }

    @Bean
    public NewTopic inventoryReservedTopic() {
        return TopicBuilder.name(InventoryTopics.INVENTORY_RESERVED).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic inventoryReservationFailedTopic() {
        return TopicBuilder.name(InventoryTopics.INVENTORY_RESERVATION_FAILED).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic inventoryReleasedTopic() {
        return TopicBuilder.name(InventoryTopics.INVENTORY_RELEASED).partitions(3).replicas(1).build();
    }
}
