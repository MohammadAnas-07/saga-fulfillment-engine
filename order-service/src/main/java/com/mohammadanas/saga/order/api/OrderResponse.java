package com.mohammadanas.saga.order.api;

import com.mohammadanas.saga.order.domain.Order;
import com.mohammadanas.saga.order.domain.OrderStatus;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * @param amount the order total. Always {@code unitPrice * quantity} — returned for
 *               convenience, never independently settable.
 */
public record OrderResponse(
        UUID id,
        String userId,
        String itemSku,
        String item,
        int quantity,
        BigDecimal unitPrice,
        BigDecimal amount,
        OrderStatus status,
        Instant createdAt,
        Instant updatedAt) {

    public static OrderResponse from(Order order) {
        return new OrderResponse(
                order.getId(),
                order.getUserId(),
                order.getItemSku(),
                order.getItem(),
                order.getQuantity(),
                order.getUnitPrice(),
                order.getAmount(),
                order.getStatus(),
                order.getCreatedAt(),
                order.getUpdatedAt());
    }
}
