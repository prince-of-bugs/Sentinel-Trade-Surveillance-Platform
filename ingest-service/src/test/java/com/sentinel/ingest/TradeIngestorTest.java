package com.sentinel.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.sentinel.common.events.NormalizedTradeEvent;
import com.sentinel.common.kafka.Headers;
import com.sentinel.common.kafka.Topics;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import tools.jackson.databind.json.JsonMapper;

class TradeIngestorTest {

    static final Instant NOW = Instant.parse("2026-11-21T14:30:01Z");

    private final JsonMapper mapper = JsonMapper.builder().build();
    private RawEventRepository rawEvents;
    private KafkaTemplate<String, String> kafka;
    private TradeIngestor ingestor;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        rawEvents = mock(RawEventRepository.class);
        kafka = mock(KafkaTemplate.class);
        when(kafka.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));
        ingestor = new TradeIngestor(new TradeEventParser(mapper), rawEvents, kafka, mapper,
                Clock.fixed(NOW, ZoneOffset.UTC));
    }

    @Test
    void validOrderIsStoredAndPublishedKeyedByAccount() {
        var rec = record(Topics.ORDERS, TradeEventParserTest.ORDER_JSON);
        rec.headers().add(new RecordHeader(Headers.CORRELATION_ID, "corr-123".getBytes(StandardCharsets.UTF_8)));

        ingestor.ingest(rec);

        RawEvent saved = savedRawEvent();
        assertThat(saved.id()).isEqualTo("trades.orders-2-42");
        assertThat(saved.status()).isEqualTo(RawEvent.Status.ACCEPTED);
        assertThat(saved.eventId()).isEqualTo("evt-1");
        assertThat(saved.payload()).isEqualTo(TradeEventParserTest.ORDER_JSON);
        assertThat(saved.receivedAt()).isEqualTo(NOW);

        ProducerRecord<String, String> out = publishedRecord();
        assertThat(out.topic()).isEqualTo(Topics.TRADES_NORMALIZED);
        assertThat(out.key()).isEqualTo("ACC-1");
        assertThat(header(out, Headers.CORRELATION_ID)).isEqualTo("corr-123");
        assertThat(header(out, Headers.EVENT_TYPE)).isEqualTo("NormalizedTradeEvent");

        NormalizedTradeEvent published = mapper.readValue(out.value(), NormalizedTradeEvent.class);
        assertThat(published.eventId()).isEqualTo("evt-1");
        assertThat(published.ingestedAt()).isEqualTo(NOW);
    }

    @Test
    void startsCorrelationIdWhenProducerDidNotSetOne() {
        ingestor.ingest(record(Topics.EXECUTIONS, TradeEventParserTest.EXECUTION_JSON));

        String generated = header(publishedRecord(), Headers.CORRELATION_ID);
        assertThat(generated).isNotBlank();
        assertThat(savedRawEvent().correlationId()).isEqualTo(generated);
    }

    @Test
    void invalidRecordIsStoredAsRejectedAndRethrownForTheDlt() {
        var rec = record(Topics.ORDERS, "{not json");

        assertThatThrownBy(() -> ingestor.ingest(rec)).isInstanceOf(InvalidEventException.class);

        RawEvent saved = savedRawEvent();
        assertThat(saved.status()).isEqualTo(RawEvent.Status.REJECTED);
        assertThat(saved.eventId()).isNull();
        assertThat(saved.rejectReason()).startsWith("unreadable JSON");
        assertThat(saved.payload()).isEqualTo("{not json");
        verifyNoInteractions(kafka);
    }

    private static ConsumerRecord<String, String> record(String topic, String value) {
        return new ConsumerRecord<>(topic, 2, 42L, "ACC-1", value);
    }

    private RawEvent savedRawEvent() {
        var captor = ArgumentCaptor.forClass(RawEvent.class);
        verify(rawEvents).save(captor.capture());
        return captor.getValue();
    }

    @SuppressWarnings("unchecked")
    private ProducerRecord<String, String> publishedRecord() {
        ArgumentCaptor<ProducerRecord<String, String>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafka).send(captor.capture());
        return captor.getValue();
    }

    private static String header(ProducerRecord<?, ?> rec, String name) {
        return new String(rec.headers().lastHeader(name).value(), StandardCharsets.UTF_8);
    }
}
