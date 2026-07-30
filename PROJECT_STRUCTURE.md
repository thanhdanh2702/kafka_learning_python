# Mapping roadmap vào cấu trúc project

| Stage | Trọng tâm | File implementation để trống |
|---:|---|---|
| 0-1 | Docker, topic, CLI | Chỉ dùng configuration và Kafka CLI |
| 2 | Python producer | `src/stage_02_producer/order_producer.py` |
| 3 | Consumer và offset | `src/stage_03_consumer/order_consumer.py` |
| 4 | Data quality | `src/stage_04_data_quality/validator.py` |
| 5 | Scale và hot partition | `src/stage_05_scaling/load_generator.py` |
| 6 | Retry, DLQ, idempotency | `retry_policy.py`, `replay_dlq.py` |
| 7 | PostgreSQL, Outbox | `postgres_sink.py`, `outbox_publisher.py`, SQL migrations |
| 8 | Retention, compaction, replay | `state_producer.py`, `projection_rebuilder.py` |
| 9 | Transaction và window | `transactional_transformer.py`, `window_aggregator.py` |
| 10 | Monitoring | `health_probe.py`, `metrics.py` |

## Shared code

Các file dùng chung cũng được để trống:

- `src/common/settings.py`
- `src/common/event_model.py`
- `src/common/kafka_factory.py`

Chỉ thêm code vào shared module khi ít nhất hai stage thực sự cần dùng chung.

## SQL migrations

- `001_create_orders.sql`
- `002_create_processed_events.sql`
- `003_create_daily_metrics.sql`
- `004_create_outbox.sql`

PostgreSQL chạy được khi các file này đang trống. Database table chỉ xuất hiện
sau khi bạn implement Stage 7 và tạo lại PostgreSQL volume của lab.
