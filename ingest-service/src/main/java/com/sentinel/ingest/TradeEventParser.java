package com.sentinel.ingest;

import com.sentinel.common.events.ExecutionEvent;
import com.sentinel.common.events.NormalizedTradeEvent;
import com.sentinel.common.events.OrderEvent;
import com.sentinel.common.kafka.Topics;
import java.time.Instant;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.exc.ValueInstantiationException;
import tools.jackson.databind.json.JsonMapper;

/**
 * Turns a raw JSON payload into a {@link NormalizedTradeEvent}. The topic decides the input type.
 * Validation lives in the event records' constructors in {@code common}, so the rules are the same
 * for producer and consumer.
 */
@Component
public class TradeEventParser {

    private final JsonMapper mapper;

    public TradeEventParser(JsonMapper mapper) {
        this.mapper = mapper;
    }

    public NormalizedTradeEvent parse(String topic, String payload, Instant ingestedAt) {
        if (payload == null || payload.isBlank()) {
            throw new InvalidEventException("empty payload");
        }
        try {
            return switch (topic) {
                case Topics.ORDERS -> NormalizedTradeEvent.from(mapper.readValue(payload, OrderEvent.class), ingestedAt);
                case Topics.EXECUTIONS ->
                        NormalizedTradeEvent.from(mapper.readValue(payload, ExecutionEvent.class), ingestedAt);
                default -> throw new InvalidEventException("unexpected topic " + topic);
            };
        } catch (ValueInstantiationException e) {
            // A record constructor rejected the values; its message names the field.
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            throw new InvalidEventException(cause.getMessage(), e);
        } catch (JacksonException e) {
            throw new InvalidEventException("unreadable JSON: " + e.getOriginalMessage(), e);
        }
    }
}
