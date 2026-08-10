package com.mohammadanas.saga.order.service;

import com.mohammadanas.saga.order.domain.Order;
import com.mohammadanas.saga.order.domain.OrderRepository;
import com.mohammadanas.saga.order.domain.OrderStatus;
import com.mohammadanas.saga.order.messaging.OrderCreatedEvent;
import com.mohammadanas.saga.order.messaging.OrderEventPublisher;
import java.math.BigDecimal;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Order lifecycle operations.
 *
 * <p>Contains no saga logic by design. This service knows how to create an order and how
 * to apply a terminal status it was told to apply; it never decides <em>which</em>
 * terminal status is correct. That decision belongs to saga-orchestrator
 * (ARCHITECTURE.md section 5.1).
 */
@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    private final OrderRepository orderRepository;
    private final OrderEventPublisher eventPublisher;

    public OrderService(OrderRepository orderRepository, OrderEventPublisher eventPublisher) {
        this.orderRepository = orderRepository;
        this.eventPublisher = eventPublisher;
    }

    /** Creates a PENDING order and announces it, which starts the saga. */
    @Transactional
    public Order createOrder(String userId, String itemSku, String item, int quantity, BigDecimal unitPrice) {
        Order order = orderRepository.save(Order.create(userId, itemSku, item, quantity, unitPrice));

        eventPublisher.publishOrderCreated(OrderCreatedEvent.from(
                order.getId(),
                order.getUserId(),
                order.getItemSku(),
                order.getItem(),
                order.getQuantity(),
                order.getUnitPrice(),
                order.getAmount()));

        log.info("Created order {} for user {}: {} x{} at {} each, total {}",
                order.getId(), userId, itemSku, quantity, order.getUnitPrice(), order.getAmount());
        return order;
    }

    @Transactional(readOnly = true)
    public Order getOrder(UUID orderId) {
        return orderRepository.findById(orderId).orElseThrow(() -> new OrderNotFoundException(orderId));
    }

    @Transactional
    public CommandOutcome confirmOrder(UUID orderId, UUID messageId) {
        return applyTerminalStatus(orderId, OrderStatus.CONFIRMED, messageId);
    }

    @Transactional
    public CommandOutcome cancelOrder(UUID orderId, UUID messageId) {
        return applyTerminalStatus(orderId, OrderStatus.CANCELLED, messageId);
    }

    /**
     * Applies a terminal status, tolerating redelivery.
     *
     * <p>The order's own status is the idempotency key here. A dedicated
     * {@code processed_message} table (ARCHITECTURE.md section 6) is not needed for this
     * service: the transition is naturally idempotent because both target statuses are
     * terminal and the order records which one it reached. inventory-service and
     * payment-service do need that table, because "reserve 3 units" and "charge $40" are
     * not self-describing in the same way.
     */
    private CommandOutcome applyTerminalStatus(UUID orderId, OrderStatus target, UUID messageId) {
        Order order = orderRepository.findById(orderId).orElse(null);

        if (order == null) {
            log.error("Ignoring {} command (messageId={}): order {} does not exist",
                    target, messageId, orderId);
            return CommandOutcome.ORDER_NOT_FOUND;
        }

        if (order.getStatus() == target) {
            log.info("Ignoring redundant {} command (messageId={}): order {} is already {}",
                    target, messageId, orderId, target);
            return CommandOutcome.DUPLICATE_IGNORED;
        }

        if (order.isTerminal()) {
            log.warn("Ignoring conflicting {} command (messageId={}): order {} is already {} "
                            + "and terminal orders never transition",
                    target, messageId, orderId, order.getStatus());
            return CommandOutcome.CONFLICT_IGNORED;
        }

        order.transitionTo(target);
        orderRepository.save(order);

        log.info("Order {} transitioned PENDING -> {} (messageId={})", orderId, target, messageId);
        return CommandOutcome.APPLIED;
    }
}
