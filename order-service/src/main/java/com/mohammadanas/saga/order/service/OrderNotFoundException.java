package com.mohammadanas.saga.order.service;

import java.util.UUID;

/** Thrown by the read API only. Command handling never throws this — see {@link CommandOutcome}. */
public class OrderNotFoundException extends RuntimeException {

    public OrderNotFoundException(UUID orderId) {
        super("Order not found: " + orderId);
    }
}
