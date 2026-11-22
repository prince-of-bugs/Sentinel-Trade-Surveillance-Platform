package com.sentinel.ingest;

import java.time.Clock;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class IngestServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(IngestServiceApplication.class, args);
    }

    /** Injected so tests can pin "now". */
    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }
}
