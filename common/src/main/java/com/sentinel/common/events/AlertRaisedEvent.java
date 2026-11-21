package com.sentinel.common.events;

import java.time.Instant;
import java.util.List;

/**
 * Output of surveillance-engine, published to {@code alerts.raised} and consumed by case-service.
 *
 * @param eventId          unique per publish; case-service stores it in {@code processed_event}
 *                         so a redelivered message never creates a second alert
 * @param evidenceEventIds the normalized trade event ids that triggered the rule
 * @param riskScore        0-100 from risk-scorer, or null if scoring was unavailable
 */
public record AlertRaisedEvent(
        String eventId,
        String alertId,
        RuleType ruleType,
        Severity severity,
        String accountId,
        String symbol,
        String summary,
        List<String> evidenceEventIds,
        Integer riskScore,
        Instant raisedAt
) {
    public AlertRaisedEvent {
        Require.notBlank(eventId, "eventId");
        Require.notBlank(alertId, "alertId");
        Require.notNull(ruleType, "ruleType");
        Require.notNull(severity, "severity");
        Require.notBlank(accountId, "accountId");
        Require.notBlank(symbol, "symbol");
        Require.notBlank(summary, "summary");
        evidenceEventIds = List.copyOf(Require.notNull(evidenceEventIds, "evidenceEventIds"));
        if (evidenceEventIds.isEmpty()) {
            throw new IllegalArgumentException("an alert needs at least one evidence event");
        }
        if (riskScore != null && (riskScore < 0 || riskScore > 100)) {
            throw new IllegalArgumentException("riskScore must be 0-100 but was " + riskScore);
        }
        Require.notNull(raisedAt, "raisedAt");
    }

    /** Returns a copy with the risk score filled in (records are immutable). */
    public AlertRaisedEvent withRiskScore(int score) {
        return new AlertRaisedEvent(eventId, alertId, ruleType, severity, accountId, symbol, summary,
                evidenceEventIds, score, raisedAt);
    }
}
