package com.sentinel.common.events;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class EventValidationTest {

    private static final Instant NOW = Instant.parse("2026-11-21T14:30:00Z");

    private static OrderEvent order(OrderAction action, long qty) {
        return new OrderEvent("evt-1", "ord-1", "ACC-1", "AAPL", Side.BUY, action,
                new BigDecimal("190.25"), qty, NOW);
    }

    @Test
    void validOrderIsAccepted() {
        OrderEvent e = order(OrderAction.CANCEL, 100);
        assertTrue(e.isCancel());
    }

    @Test
    void orderRejectsNonPositiveQuantity() {
        assertThrows(IllegalArgumentException.class, () -> order(OrderAction.NEW, 0));
    }

    @Test
    void orderRejectsBlankAccount() {
        assertThrows(IllegalArgumentException.class, () -> new OrderEvent("evt-1", "ord-1", " ", "AAPL",
                Side.BUY, OrderAction.NEW, BigDecimal.ONE, 1, NOW));
    }

    @Test
    void executionDetectsSelfTrade() {
        ExecutionEvent e = new ExecutionEvent("evt-2", "exe-1", "ord-1", "ACC-1", "ACC-1", "AAPL",
                Side.SELL, new BigDecimal("190.30"), 100, NOW);
        assertTrue(e.isSelfTrade());
    }

    @Test
    void priceTickRejectsCrossedBook() {
        assertThrows(IllegalArgumentException.class, () -> new PriceTick("evt-3", "AAPL",
                new BigDecimal("190.31"), new BigDecimal("190.30"), NOW));
    }

    @Test
    void priceTickMid() {
        PriceTick t = new PriceTick("evt-3", "AAPL", new BigDecimal("190.30"), new BigDecimal("190.31"), NOW);
        assertEquals(new BigDecimal("190.305"), t.mid());
    }

    @Test
    void normalizesOrderAndExecution() {
        Instant ingested = NOW.plusMillis(5);
        NormalizedTradeEvent fromOrder = NormalizedTradeEvent.from(order(OrderAction.NEW, 100), ingested);
        assertEquals(NormalizedTradeEvent.Kind.ORDER, fromOrder.kind());
        assertNull(fromOrder.counterpartyAccountId());

        ExecutionEvent exec = new ExecutionEvent("evt-2", "exe-1", "ord-1", "ACC-1", "ACC-2", "AAPL",
                Side.BUY, new BigDecimal("190.30"), 100, NOW);
        NormalizedTradeEvent fromExec = NormalizedTradeEvent.from(exec, ingested);
        assertEquals(NormalizedTradeEvent.Kind.EXECUTION, fromExec.kind());
        assertNull(fromExec.action());
        assertEquals("ACC-2", fromExec.counterpartyAccountId());
    }

    @Test
    void alertCopiesEvidenceDefensively() {
        List<String> evidence = new ArrayList<>(List.of("evt-1"));
        AlertRaisedEvent alert = new AlertRaisedEvent("evt-9", "alr-1", RuleType.SPOOFING, Severity.HIGH,
                "ACC-1", "AAPL", "Large order cancelled within 2s", evidence, null, NOW);
        evidence.add("evt-2");
        assertEquals(1, alert.evidenceEventIds().size());
        assertEquals(Integer.valueOf(87), alert.withRiskScore(87).riskScore());
    }

    @Test
    void alertRejectsOutOfRangeScore() {
        assertThrows(IllegalArgumentException.class, () -> new AlertRaisedEvent("evt-9", "alr-1",
                RuleType.SPOOFING, Severity.HIGH, "ACC-1", "AAPL", "x", List.of("evt-1"), 101, NOW));
    }
}
