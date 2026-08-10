package com.mohammadanas.saga.order.api;

import com.mohammadanas.saga.order.domain.Order;
import com.mohammadanas.saga.order.domain.OrderStatus;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record OrderResponse(
        UUID id,
        String userId,
        String item,
        BigDecimal amount,
        OrderStatus status,
        Instant createdAt,
        Instant updatedAt) {

    public static OrderResponse from(Order order) {
        return new OrderResponse(
                order.getId(),
                order.getUserId(),
                order.getItem(),
                order.getAmount(),
                order.getStatus(),
                order.getCreatedAt(),
                order.getUpdatedAt());
    }
}
