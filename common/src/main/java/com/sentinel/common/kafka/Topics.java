package com.sentinel.common.kafka;

import java.util.List;

/**
 * Every Kafka topic in the system. Keep this list in sync with infra/kafka/create-topics.sh;
 * TopicsTest fails the build if the two drift apart.
 */
public final class Topics {

    private Topics() {
    }

    public static final String ORDERS = "trades.orders";
    public static final String EXECUTIONS = "trades.executions";
    public static final String PRICES = "market.prices";
    public static final String TRADES_NORMALIZED = "trades.normalized";
    public static final String ALERTS_RAISED = "alerts.raised";
    public static final String TRADES_DLT = "trades.dlt";
    public static final String ALERTS_DLT = "alerts.dlt";

    public static final List<String> ALL = List.of(
            ORDERS, EXECUTIONS, PRICES, TRADES_NORMALIZED, ALERTS_RAISED, TRADES_DLT, ALERTS_DLT);
}
