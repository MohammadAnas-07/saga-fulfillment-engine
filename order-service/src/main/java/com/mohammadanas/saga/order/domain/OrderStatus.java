package com.mohammadanas.saga.order.domain;

/**
 * Lifecycle of an {@link Order}.
 *
 * <p>{@code PENDING} is the only non-terminal state. Once an order reaches
 * {@code CONFIRMED} or {@code CANCELLED} it never transitions again — that property is
 * what makes redundant commands safe to ignore (see ARCHITECTURE.md section 6).
 */
public enum OrderStatus {

    PENDING,
    CONFIRMED,
    CANCELLED;

    public boolean isTerminal() {
        return this != PENDING;
    }
}
