package com.sentinel.common.kafka;

/** Kafka header names shared by every producer and consumer. */
public final class Headers {

    private Headers() {
    }

    /** Set by the first producer and copied onto every downstream message, so logs can be joined. */
    public static final String CORRELATION_ID = "x-correlation-id";

    /** Simple class name of the payload record, e.g. "OrderEvent". */
    public static final String EVENT_TYPE = "x-event-type";
}
