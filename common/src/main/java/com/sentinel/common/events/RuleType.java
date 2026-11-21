package com.sentinel.common.events;

/** Surveillance rules the engine can raise alerts for. */
public enum RuleType {
    EXCESSIVE_CANCELS,
    SPOOFING,
    LAYERING,
    WASH_TRADING
}
