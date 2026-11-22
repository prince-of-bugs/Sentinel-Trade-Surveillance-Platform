package com.sentinel.ingest;

import com.sentinel.common.events.NormalizedTradeEvent;
import com.sentinel.common.kafka.Headers;
import com.sentinel.common.kafka.Topics;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import tools.jackson.databind.json.JsonMapper;

/**
 * Consumes raw orders and executions. For each record it:
 * <ol>
 *   <li>parses and validates it into a {@link NormalizedTradeEvent},</li>
 *   <li>stores the raw payload in MongoDB,</li>
 *   <li>publishes the normalized event to {@code trades.normalized}, keyed by accountId.</li>
 * </ol>
 * The offset is committed only after all three succeed, so a crash means redelivery, not loss
 * (at-least-once). Invalid records are stored as REJECTED and the {@link InvalidEventException} is
 * rethrown, so the error handler in {@link KafkaConfig} sends them to {@code trades.dlt}.
 */
@Service
public class TradeIngestor {

    private static final Logger log = LoggerFactory.getLogger(TradeIngestor.class);
    private static final String MDC_CORRELATION_ID = "correlationId";

    private final TradeEventParser parser;
    private final RawEventRepository rawEvents;
    private final KafkaTemplate<String, String> kafka;
    private final JsonMapper mapper;
    private final Clock clock;

    public TradeIngestor(TradeEventParser parser, RawEventRepository rawEvents, KafkaTemplate<String, String> kafka,
                         JsonMapper mapper, Clock clock) {
        this.parser = parser;
        this.rawEvents = rawEvents;
        this.kafka = kafka;
        this.mapper = mapper;
        this.clock = clock;
    }

    @KafkaListener(id = "ingest", topics = {Topics.ORDERS, Topics.EXECUTIONS})
    public void ingest(ConsumerRecord<String, String> rec) {
        Instant now = clock.instant();
        String correlationId = correlationId(rec);
        MDC.put(MDC_CORRELATION_ID, correlationId);
        try {
            NormalizedTradeEvent event;
            try {
                event = parser.parse(rec.topic(), rec.value(), now);
            } catch (InvalidEventException e) {
                rawEvents.save(RawEvent.rejected(rec, correlationId, e.getMessage(), now));
                log.warn("Rejected {}-{}@{}: {}", rec.topic(), rec.partition(), rec.offset(), e.getMessage());
                throw e;
            }
            rawEvents.save(RawEvent.accepted(rec, event.eventId(), correlationId, now));
            publish(event, correlationId);
            log.debug("Ingested {} {} for {}", event.kind(), event.eventId(), event.accountId());
        } finally {
            MDC.remove(MDC_CORRELATION_ID);
        }
    }

    private void publish(NormalizedTradeEvent event, String correlationId) {
        var out = new ProducerRecord<String, String>(Topics.TRADES_NORMALIZED, event.accountId(),
                mapper.writeValueAsString(event));
        out.headers()
                .add(Headers.CORRELATION_ID, correlationId.getBytes(StandardCharsets.UTF_8))
                .add(Headers.EVENT_TYPE, NormalizedTradeEvent.class.getSimpleName().getBytes(StandardCharsets.UTF_8));
        // Wait for the broker ack so the offset is never committed ahead of the publish.
        kafka.send(out).join();
    }

    /** Uses the producer's correlation id, or starts one if the producer didn't set it. */
    private static String correlationId(ConsumerRecord<?, ?> rec) {
        Header header = rec.headers().lastHeader(Headers.CORRELATION_ID);
        if (header != null && header.value() != null && header.value().length > 0) {
            return new String(header.value(), StandardCharsets.UTF_8);
        }
        return UUID.randomUUID().toString();
    }
}
