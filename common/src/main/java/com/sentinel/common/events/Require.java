package com.sentinel.common.events;

import java.math.BigDecimal;
import java.util.Objects;

/** Small guard helpers used by the event records' compact constructors. */
final class Require {

    private Require() {
    }

    static String notBlank(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return value;
    }

    static <T> T notNull(T value, String field) {
        return Objects.requireNonNull(value, field + " must not be null");
    }

    static long positive(long value, String field) {
        if (value <= 0) {
            throw new IllegalArgumentException(field + " must be > 0 but was " + value);
        }
        return value;
    }

    static BigDecimal positive(BigDecimal value, String field) {
        notNull(value, field);
        if (value.signum() <= 0) {
            throw new IllegalArgumentException(field + " must be > 0 but was " + value);
        }
        return value;
    }
}
