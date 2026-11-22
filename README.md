# Sentinel

A trade surveillance platform built by Prince Gurung and Sushant Gurung. A simulator streams trades into
Kafka, a Java rule engine flags spoofing, layering and wash trading, and analysts investigate the alerts in
a web UI.

> Status: **Week 1, foundation.** Shared event schemas, local infrastructure, CI and the case-service
> database baseline are in place. Services arrive from week 2.

## Architecture

```
market-sim ──trades.*──▶ ingest-service ──trades.normalized──▶ surveillance-engine ──alerts.raised──▶ case-service ◀── Web UIs
                              │                                     │        │                        │
                           MongoDB                                Redis  risk-scorer              PostgreSQL
```

Full plan, diagrams and week-by-week schedule: *link to the shared plan doc*.

## Tech

Java 21 · Spring Boot 4 · Apache Kafka (KRaft) · PostgreSQL 17 · MongoDB 8 · Redis 7 · Flyway ·
Angular · React · Python (FastAPI) · Docker · GitHub Actions · AWS

## Quick start

Prerequisites: JDK 21, Maven 3.9+, Docker Desktop (or Docker Engine with Compose v2).

```bash
# 1. Start Kafka, Postgres, Mongo, Redis and create all topics
docker compose -f infra/docker-compose.yml up -d

# 2. Check the topics were created
docker logs sentinel-kafka-init          # ends with "All topics ready."
open http://localhost:8090               # Kafka UI (use xdg-open on Linux)

# 3. Build everything and run the tests
mvn verify

# 4. Run case-service (applies the Flyway migrations to Postgres)
mvn -pl case-service spring-boot:run
curl localhost:8083/actuator/health      # {"status":"UP",...}

# 5. Run ingest-service (consumes trades.orders / trades.executions)
mvn -pl ingest-service spring-boot:run
curl localhost:8082/actuator/health
```

Until market-sim lands you can feed ingest-service by hand from the Kafka container:

```bash
docker exec -it sentinel-kafka /opt/kafka/bin/kafka-console-producer.sh   --bootstrap-server localhost:9092 --topic trades.orders   --property parse.key=true --property key.separator='|'
ACC-1|{"eventId":"evt-1","orderId":"ord-1","accountId":"ACC-1","symbol":"AAPL","side":"BUY","action":"NEW","price":190.25,"quantity":100,"occurredAt":"2026-11-21T14:30:00Z"}
```

The record shows up in Kafka UI on `trades.normalized` and in Mongo:
`docker exec sentinel-mongo mongosh sentinel --quiet --eval 'db.raw_events.find().limit(5)'`.
Send something that isn't valid JSON and it lands on `trades.dlt` instead, stored in Mongo as `REJECTED`.

Stop with `docker compose -f infra/docker-compose.yml down` (add `-v` to wipe data).

## Repository layout

| Path | What | Owner |
| --- | --- | --- |
| `common/` | Event records and topic names shared by every service | Both |
| `case-service/` | Alerts, cases, notes, audit, AI assistant; owns the Postgres schema | Sushant |
| `infra/docker-compose.yml` | Local stack | Sushant |
| `infra/kafka/create-topics.sh` | Topic definitions | Both |
| `.github/workflows/` | CI | Prince |
| `ingest-service/` | Validation, normalization, raw-event store (Mongo), DLT | Prince |
| `market-sim/` *(week 2)* | Multithreaded trade simulator | Sushant |
| `surveillance-engine/` *(week 3)* | Rule engine | Prince |

## Ports

| Service | Port |
| --- | --- |
| market-sim | 8081 |
| ingest-service | 8082 |
| case-service | 8083 |
| surveillance-engine | 8084 |
| risk-scorer | 8085 |
| Kafka UI | 8090 |
| Kafka | 9092 |
| PostgreSQL | 5432 |
| MongoDB | 27017 |
| Redis | 6379 |

## Kafka topics

| Topic | Key | Partitions | Producer → Consumer |
| --- | --- | --- | --- |
| `trades.orders` | accountId | 6 | market-sim → ingest-service |
| `trades.executions` | accountId | 6 | market-sim → ingest-service |
| `market.prices` | symbol | 3 | market-sim → surveillance-engine |
| `trades.normalized` | accountId | 6 | ingest-service → surveillance-engine |
| `alerts.raised` | alertId | 3 | surveillance-engine → case-service |
| `trades.dlt`, `alerts.dlt` | original key | 1 | failed messages after retries |

Payloads are JSON with the same field names as the records in `common/.../events`. Timestamps are
ISO-8601 strings and enums are their names. Producers should set the `x-correlation-id` header;
ingest-service starts one if it's missing and copies it onto everything downstream.

To add a topic, update **both** `common/.../kafka/Topics.java` and `infra/kafka/create-topics.sh`.
`TopicsTest` fails the build if they drift apart.

## How we work

- `main` is protected: every change goes through a pull request with one approval and green CI.
- Branches: `feat/<issue>-short-name`, `fix/<issue>-short-name`. Squash-merge, then delete the branch.
- Commits follow [Conventional Commits](https://www.conventionalcommits.org): `feat:`, `fix:`, `test:`, `docs:`, `chore:`.
- Database changes are new Flyway files (`V2__add_x.sql`). Never edit a merged migration.
- Changes to `common/` need both of us to approve, because every service depends on it.
