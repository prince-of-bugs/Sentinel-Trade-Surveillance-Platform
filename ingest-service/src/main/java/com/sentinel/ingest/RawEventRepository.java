package com.sentinel.ingest;

import org.springframework.data.mongodb.repository.MongoRepository;

public interface RawEventRepository extends MongoRepository<RawEvent, String> {
}
