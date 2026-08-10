package com.mohammadanas.saga.order.api;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.math.BigDecimal;

/**
 * @param itemSku the inventory item identifier, in the same space as inventory-service's
 *                {@code itemId}. Required, because the saga cannot reserve stock without
 *                it.
 * @param item    free-text description for display only. Never used to look up stock.
 */
public record CreateOrderRequest(
        @NotBlank String userId,
        @NotBlank String itemSku,
        @NotBlank String item,
        @Min(value = 1, message = "quantity must be at least 1") int quantity,
        @NotNull @DecimalMin(value = "0.01", message = "amount must be greater than zero") BigDecimal amount) {
}
