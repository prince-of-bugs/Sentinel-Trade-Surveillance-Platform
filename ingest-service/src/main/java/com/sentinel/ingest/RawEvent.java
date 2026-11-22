package com.sentinel.ingest;

import java.time.Instant;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * Every record ingest-service receives, valid or not, exactly as it arrived. Kept 30 days for replay
 * and evidence lookups.
 *
 * @param id      {@code topic-partition-offset}, so redelivering the same Kafka record overwrites
 *                its document instead of adding a second one
 * @param eventId null when the payload couldn't be parsed
 */
@Document("raw_events")
public record RawEvent(
        @Id String id,
        @Indexed String eventId,
        String topic,
        int partition,
        long offset,
        String key,
        String correlationId,
        Status status,
        String rejectReason,
        String payload,
        @Indexed(expireAfter = "30d") Instant receivedAt
) {
    public enum Status { ACCEPTED, REJECTED }

    static RawEvent accepted(ConsumerRecord<String, String> rec, String eventId, String correlationId,
                             Instant receivedAt) {
        return of(rec, eventId, correlationId, Status.ACCEPTED, null, receivedAt);
    }

    static RawEvent rejected(ConsumerRecord<String, String> rec, String correlationId, String reason,
                             Instant receivedAt) {
        return of(rec, null, correlationId, Status.REJECTED, reason, receivedAt);
    }

    private static RawEvent of(ConsumerRecord<String, String> rec, String eventId, String correlationId,
                               Status status, String reason, Instant receivedAt) {
        String id = rec.topic() + "-" + rec.partition() + "-" + rec.offset();
        return new RawEvent(id, eventId, rec.topic(), rec.partition(), rec.offset(), rec.key(), correlationId,
                status, reason, rec.value(), receivedAt);
    }
}
