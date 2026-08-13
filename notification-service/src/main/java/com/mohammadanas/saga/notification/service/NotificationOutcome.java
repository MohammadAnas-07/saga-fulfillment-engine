package com.mohammadanas.saga.notification.service;

/**
 * Result of handling an event, so tests can distinguish "notified" from "correctly
 * declined to".
 */
public enum NotificationOutcome {

    /** A notification was rendered and handed to the sender. */
    SENT,

    /**
     * The event's messageId had already been notified on. Nothing was sent — the outcome
     * that stops a redelivery becoming a second message to the customer.
     */
    DUPLICATE_IGNORED
}
