package com.sentinel.ingest;

import com.sentinel.common.kafka.Topics;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConfig {

    /**
     * Boot wires this into the listener container. Invalid payloads go straight to {@code trades.dlt};
     * anything else (Mongo or broker hiccups) is retried a few times first. Spring Kafka adds
     * {@code kafka_dlt-*} headers with the original topic, offset and exception, and keeps the
     * original headers, correlation id included.
     * <p>
     * Week 4 replaces the fixed backoff with exponential backoff and adds poison-message tests.
     */
    @Bean
    DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> template) {
        // Partition -1 lets the producer pick, since trades.dlt has one partition and the source has six.
        var recoverer = new DeadLetterPublishingRecoverer(template,
                (rec, ex) -> new TopicPartition(Topics.TRADES_DLT, -1));
        var handler = new DefaultErrorHandler(recoverer, new FixedBackOff(500L, 3L));
        handler.addNotRetryableExceptions(InvalidEventException.class);
        return handler;
    }
}
