package com.mohammadanas.saga.notification.service;

import java.util.UUID;

/**
 * A customer notification, rendered but not yet delivered.
 *
 * <p>Modelled as a value rather than composed inline in a log statement so the rendered
 * message is assertable in tests without capturing log output.
 */
public record Notification(
        NotificationType type,
        UUID orderId,
        String userId,
        String message) {
}
