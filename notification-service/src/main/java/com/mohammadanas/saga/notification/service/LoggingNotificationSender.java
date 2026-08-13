package com.mohammadanas.saga.notification.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * The simulated channel: writes the notification to the log and nothing else.
 *
 * <p>No email, no SMS, no push — see {@link NotificationSender}.
 */
@Component
public class LoggingNotificationSender implements NotificationSender {

    private static final Logger log = LoggerFactory.getLogger(LoggingNotificationSender.class);

    @Override
    public void send(Notification notification) {
        log.info("NOTIFICATION [{}] to user {} for order {}: {}",
                notification.type(), notification.userId(), notification.orderId(), notification.message());
    }
}
