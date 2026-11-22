package com.sentinel.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.sentinel.common.kafka.Headers;
import com.sentinel.common.kafka.Topics;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.mongodb.MongoDBContainer;

/**
 * End to end against a real Kafka broker and MongoDB. Skipped when Docker isn't available; CI
 * always has it.
 */
@SpringBootTest
@Testcontainers(disabledWithoutDocker = true)
class IngestPipelineIntegrationTest {

    @Container
    @ServiceConnection
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.1.0");

    @Container
    @ServiceConnection
    static final MongoDBContainer MONGO = new MongoDBContainer("mongo:8.0");

    /** Production topics come from create-topics.sh; here Boot's KafkaAdmin creates them on startup. */
    @TestConfiguration
    static class TestTopics {
        @Bean NewTopic orders() { return new NewTopic(Topics.ORDERS, 6, (short) 1); }
        @Bean NewTopic executions() { return new NewTopic(Topics.EXECUTIONS, 6, (short) 1); }
        @Bean NewTopic normalized() { return new NewTopic(Topics.TRADES_NORMALIZED, 6, (short) 1); }
        @Bean NewTopic dlt() { return new NewTopic(Topics.TRADES_DLT, 1, (short) 1); }
    }

    @Autowired
    KafkaTemplate<String, String> kafka;

    @Autowired
    RawEventRepository rawEvents;

    @Test
    void validOrderIsStoredInMongoAndPublishedNormalized() {
        String eventId = "evt-" + UUID.randomUUID();
        String json = TradeEventParserTest.ORDER_JSON.replace("evt-1", eventId);
        var rec = new ProducerRecord<>(Topics.ORDERS, "ACC-1", json);
        rec.headers().add(Headers.CORRELATION_ID, "corr-it-1".getBytes(StandardCharsets.UTF_8));
        kafka.send(rec).join();

        ConsumerRecord<String, String> out = awaitRecord(Topics.TRADES_NORMALIZED, v -> v.contains(eventId));
        assertThat(out.key()).isEqualTo("ACC-1");
        assertThat(new String(out.headers().lastHeader(Headers.CORRELATION_ID).value(), StandardCharsets.UTF_8))
                .isEqualTo("corr-it-1");

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(rawEvents.findAll())
                        .anySatisfy(r -> {
                            assertThat(r.eventId()).isEqualTo(eventId);
                            assertThat(r.status()).isEqualTo(RawEvent.Status.ACCEPTED);
                            assertThat(r.correlationId()).isEqualTo("corr-it-1");
                        }));
    }

    @Test
    void malformedRecordGoesToDltAndIsStoredAsRejected() {
        String garbage = "{garbage-" + UUID.randomUUID();
        kafka.send(Topics.ORDERS, "ACC-9", garbage).join();

        ConsumerRecord<String, String> dead = awaitRecord(Topics.TRADES_DLT, garbage::equals);
        assertThat(dead.key()).isEqualTo("ACC-9");
        assertThat(dead.headers().lastHeader("kafka_dlt-original-topic").value())
                .asString(StandardCharsets.UTF_8).isEqualTo(Topics.ORDERS);

        assertThat(rawEvents.findAll())
                .anySatisfy(r -> {
                    assertThat(r.payload()).isEqualTo(garbage);
                    assertThat(r.status()).isEqualTo(RawEvent.Status.REJECTED);
                    assertThat(r.rejectReason()).startsWith("unreadable JSON");
                });
    }

    private static ConsumerRecord<String, String> awaitRecord(String topic,
                                                              java.util.function.Predicate<String> matches) {
        Map<String, Object> props = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "it-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (var consumer = new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(List.of(topic));
            List<ConsumerRecord<String, String>> found = new ArrayList<>();
            await().atMost(Duration.ofSeconds(30)).until(() -> {
                consumer.poll(Duration.ofMillis(500)).forEach(r -> {
                    if (r.value() != null && matches.test(r.value())) {
                        found.add(r);
                    }
                });
                return !found.isEmpty();
            });
            return found.getFirst();
        }
    }
}
