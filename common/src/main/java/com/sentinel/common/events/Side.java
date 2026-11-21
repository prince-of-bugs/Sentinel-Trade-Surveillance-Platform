package com.sentinel.common.events;

/** Which side of the book an order or execution is on. */
public enum Side {
    BUY,
    SELL;

    public Side opposite() {
        return this == BUY ? SELL : BUY;
    }
}
