package com.sentinel.common.events;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * An order placed, amended or cancelled. Published by market-sim to {@code trades.orders},
 * keyed by {@code accountId} so each account's events stay in order on one partition.
 *
 * @param eventId    globally unique id, used for de-duplication downstream
 * @param orderId    the order this event belongs to (same across NEW / AMEND / CANCEL)
 * @param price      limit price; must be positive
 * @param quantity   order size in shares or units; must be positive
 */
public record OrderEvent(
        String eventId,
        String orderId,
        String accountId,
        String symbol,
        Side side,
        OrderAction action,
        BigDecimal price,
        long quantity,
        Instant occurredAt
) {
    public OrderEvent {
        Require.notBlank(eventId, "eventId");
        Require.notBlank(orderId, "orderId");
        Require.notBlank(accountId, "accountId");
        Require.notBlank(symbol, "symbol");
        Require.notNull(side, "side");
        Require.notNull(action, "action");
        Require.positive(price, "price");
        Require.positive(quantity, "quantity");
        Require.notNull(occurredAt, "occurredAt");
    }

    public boolean isCancel() {
        return action == OrderAction.CANCEL;
    }
}
