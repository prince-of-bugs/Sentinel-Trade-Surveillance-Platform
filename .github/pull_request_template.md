## What

<!-- One or two sentences. Link the issue: Closes #12 -->

## Why

## How I tested

- [ ] `mvn verify` passes locally
- [ ] Ran against `docker compose` stack (if it touches Kafka, Postgres, Mongo or Redis)
- [ ] Added or updated tests

## Checklist

- [ ] Event schema changes in `common/` are backward compatible, or both of us agreed on the break
- [ ] New Kafka topic added to both `Topics.java` and `infra/kafka/create-topics.sh`
- [ ] No secrets, keys or `.env` files committed
