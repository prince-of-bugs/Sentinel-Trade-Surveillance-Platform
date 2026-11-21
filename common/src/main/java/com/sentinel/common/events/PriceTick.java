package com.sentinel.common.events;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;

/** Top-of-book quote. Published by market-sim to {@code market.prices}, keyed by {@code symbol}. */
public record PriceTick(
        String eventId,
        String symbol,
        BigDecimal bid,
        BigDecimal ask,
        Instant occurredAt
) {
    public PriceTick {
        Require.notBlank(eventId, "eventId");
        Require.notBlank(symbol, "symbol");
        Require.positive(bid, "bid");
        Require.positive(ask, "ask");
        if (bid.compareTo(ask) > 0) {
            throw new IllegalArgumentException("bid " + bid + " must not exceed ask " + ask);
        }
        Require.notNull(occurredAt, "occurredAt");
    }

    public BigDecimal mid() {
        return bid.add(ask).divide(BigDecimal.TWO, Math.max(bid.scale(), ask.scale()) + 1, RoundingMode.HALF_EVEN);
    }
}
