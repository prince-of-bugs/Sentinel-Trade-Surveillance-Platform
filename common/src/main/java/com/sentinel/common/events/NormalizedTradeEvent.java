package com.sentinel.common.events;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * One flat shape for orders and executions after ingest-service has validated and de-duplicated
 * them. Published to {@code trades.normalized}, keyed by {@code accountId}. A flat record keeps
 * the rule engine simple: every rule reads the same fields.
 *
 * @param kind                  ORDER or EXECUTION
 * @param action                set for orders; null for executions
 * @param counterpartyAccountId set for executions; null for orders
 * @param ingestedAt            when ingest-service accepted the event
 */
public record NormalizedTradeEvent(
        String eventId,
        Kind kind,
        String orderId,
        String accountId,
        String counterpartyAccountId,
        String symbol,
        Side side,
        OrderAction action,
        BigDecimal price,
        long quantity,
        Instant occurredAt,
        Instant ingestedAt
) {
    public enum Kind { ORDER, EXECUTION }

    public NormalizedTradeEvent {
        Require.notBlank(eventId, "eventId");
        Require.notNull(kind, "kind");
        Require.notBlank(orderId, "orderId");
        Require.notBlank(accountId, "accountId");
        Require.notBlank(symbol, "symbol");
        Require.notNull(side, "side");
        Require.positive(price, "price");
        Require.positive(quantity, "quantity");
        Require.notNull(occurredAt, "occurredAt");
        Require.notNull(ingestedAt, "ingestedAt");
        if (kind == Kind.ORDER && action == null) {
            throw new IllegalArgumentException("action is required for ORDER events");
        }
        if (kind == Kind.EXECUTION && counterpartyAccountId == null) {
            throw new IllegalArgumentException("counterpartyAccountId is required for EXECUTION events");
        }
    }

    public static NormalizedTradeEvent from(OrderEvent e, Instant ingestedAt) {
        return new NormalizedTradeEvent(e.eventId(), Kind.ORDER, e.orderId(), e.accountId(), null,
                e.symbol(), e.side(), e.action(), e.price(), e.quantity(), e.occurredAt(), ingestedAt);
    }

    public static NormalizedTradeEvent from(ExecutionEvent e, Instant ingestedAt) {
        return new NormalizedTradeEvent(e.eventId(), Kind.EXECUTION, e.orderId(), e.accountId(),
                e.counterpartyAccountId(), e.symbol(), e.side(), null, e.price(), e.quantity(),
                e.occurredAt(), ingestedAt);
    }
}
