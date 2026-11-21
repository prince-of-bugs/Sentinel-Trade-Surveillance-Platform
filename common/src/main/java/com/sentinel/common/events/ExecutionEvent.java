package com.sentinel.common.events;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * A fill against an order. Published by market-sim to {@code trades.executions}, keyed by
 * {@code accountId}. {@code counterpartyAccountId} is what the wash-trading rule looks at.
 */
public record ExecutionEvent(
        String eventId,
        String executionId,
        String orderId,
        String accountId,
        String counterpartyAccountId,
        String symbol,
        Side side,
        BigDecimal price,
        long quantity,
        Instant occurredAt
) {
    public ExecutionEvent {
        Require.notBlank(eventId, "eventId");
        Require.notBlank(executionId, "executionId");
        Require.notBlank(orderId, "orderId");
        Require.notBlank(accountId, "accountId");
        Require.notBlank(counterpartyAccountId, "counterpartyAccountId");
        Require.notBlank(symbol, "symbol");
        Require.notNull(side, "side");
        Require.positive(price, "price");
        Require.positive(quantity, "quantity");
        Require.notNull(occurredAt, "occurredAt");
    }

    /** True when both sides of the trade are the same account (the simplest wash trade). */
    public boolean isSelfTrade() {
        return accountId.equals(counterpartyAccountId);
    }
}
