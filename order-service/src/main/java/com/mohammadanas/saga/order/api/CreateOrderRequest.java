package com.mohammadanas.saga.order.api;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.math.BigDecimal;

public record CreateOrderRequest(
        @NotBlank String userId,
        @NotBlank String item,
        @NotNull @DecimalMin(value = "0.01", message = "amount must be greater than zero") BigDecimal amount) {
}
