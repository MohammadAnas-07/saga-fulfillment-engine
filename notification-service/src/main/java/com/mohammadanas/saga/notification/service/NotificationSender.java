package com.mohammadanas.saga.notification.service;

/**
 * Delivers a rendered notification.
 *
 * <p>The seam where a real channel would attach. **Real email and SMS delivery are out of
 * scope** for this project (ARCHITECTURE.md section 1) — the only implementation logs.
 * Keeping it an interface means the simulated output can be asserted directly instead of
 * through a log appender, and means adding a real channel later does not touch the
 * consumer or the dedup logic.
 */
public interface NotificationSender {

    void send(Notification notification);
}
