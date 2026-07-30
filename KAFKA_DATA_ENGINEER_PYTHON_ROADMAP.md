# Lộ trình học Apache Kafka cho Data Engineer bằng Python

> Study case xuyên suốt: xây dựng pipeline xử lý đơn hàng thương mại điện tử theo
> thời gian thực bằng Apache Kafka, Python, Docker và PostgreSQL.

## 1. Mục tiêu của lộ trình

Lộ trình này dành cho người mới bắt đầu. Mục tiêu không phải là viết một hệ
thống lớn thật nhanh, mà là tự quan sát và giải thích được Kafka hoạt động như
thế nào trong một pipeline dữ liệu thực tế.

Sau khi hoàn thành, bạn có thể:

- Giải thích broker, controller, topic, partition, offset, key và consumer group.
- Viết producer và consumer Python bằng `confluent-kafka`.
- Chọn message key để cân bằng giữa ordering và khả năng xử lý song song.
- Phân biệt current position, committed offset và consumer lag.
- Xây dựng pipeline raw -> validated -> PostgreSQL.
- Xử lý duplicate bằng idempotency.
- Phân loại lỗi, retry hữu hạn, đưa poison message vào DLQ và replay an toàn.
- Phân biệt retention với log compaction.
- Hiểu phạm vi của at-most-once, at-least-once và exactly-once.
- Giải thích dual-write problem và Transactional Outbox.
- Biết các metric, cảnh báo và thay đổi cần có khi tiến gần production.

### Phạm vi được cố ý giữ đơn giản

- Một Kafka broker chạy KRaft trong Docker.
- Payload JSON dễ đọc bằng mắt.
- Python thuần, chưa dùng web framework.
- PostgreSQL chỉ xuất hiện sau khi đã hiểu producer, consumer và offset.
- Không dùng Kubernetes, Schema Registry, Flink hay Spark trong phần bắt buộc.
- Không xây frontend, authentication, thanh toán hoặc email thật.

Độ khó của khóa học phải đến từ Kafka, không đến từ một application framework
phức tạp.

---

## 2. Phiên bản và nguồn tham khảo

Phiên bản được kiểm tra ngày **30-07-2026**:

| Công cụ | Phiên bản dùng trong lộ trình | Ghi chú |
|---|---:|---|
| Apache Kafka | `4.3.1` | Bản ổn định hiện hành, chạy KRaft, không dùng ZooKeeper |
| Python | `3.14.6` | Dùng image chính thức `python:3.14.6-slim` |
| confluent-kafka | `2.15.0` | Python client dựa trên `librdkafka` |
| PostgreSQL | `18.4` | Chỉ cần từ Stage 7 |
| psycopg | `3.3.4` | PostgreSQL client cho Python |
| Docker Compose | Compose Specification | Không khai báo khóa `version` đã lỗi thời |

Nên pin phiên bản như trên để kết quả thực hành có thể lặp lại. Khi học vào thời
điểm khác, kiểm tra bản vá mới rồi cập nhật từng phiên bản có chủ đích; không đổi
toàn bộ công cụ giữa một stage.

Nguồn hiện hành:

- [Apache Kafka downloads](https://kafka.apache.org/community/downloads/)
- [Apache Kafka 4.3 documentation](https://kafka.apache.org/43/)
- [Apache Kafka quickstart](https://kafka.apache.org/quickstart/)
- [Apache Kafka Docker examples](https://github.com/apache/kafka/tree/trunk/docker/examples)
- [confluent-kafka 2.15.0 API](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html)
- [confluent-kafka trên PyPI](https://pypi.org/project/confluent-kafka/)
- [Python 3.14.6 documentation](https://docs.python.org/3.14/)
- [Docker Compose Specification](https://docs.docker.com/reference/compose-file/)
- [PostgreSQL official Docker image](https://hub.docker.com/_/postgres)
- [psycopg trên PyPI](https://pypi.org/project/psycopg/)

### Hai sách dùng để đào sâu

**Kafka: The Definitive Guide, Second Edition**

| Nội dung | Chương nên đọc |
|---|---|
| Tổng quan | Chapter 1 |
| Producer | Chapter 3 |
| Consumer, group, offset, rebalance | Chapter 4 |
| Partition, replication, storage, compaction | Chapter 6 |
| Reliable delivery | Chapter 7 |
| Idempotence và transaction | Chapter 8 |
| Data pipeline và Kafka Connect | Chapter 9 |
| Monitoring | Chapter 13 |
| Stream processing | Chapter 14 |

Phần cài đặt trong sách còn nói nhiều về ZooKeeper. Kafka 4.x đã chuyển hoàn
toàn sang KRaft, vì vậy chỉ đọc phần ZooKeeper để hiểu lịch sử, không làm theo
cách cài cũ.

**Designing Data-Intensive Applications**

| Nội dung | Chương nên đọc |
|---|---|
| Reliability, scalability, maintainability | Chapter 1 |
| Encoding và schema evolution | Chapter 4 |
| Replication | Chapter 5 |
| Partitioning | Chapter 6 |
| Transactions | Chapter 7 |
| Sự cố trong hệ phân tán | Chapter 8 |
| Batch processing | Chapter 10 |
| Stream processing | Chapter 11 |

---

## 3. Study case: Real-time E-commerce Order Pipeline

Một hệ thống bán hàng liên tục phát sinh đơn hàng. Data Engineering team cần:

1. Nhận event đơn hàng thô.
2. Kiểm tra dữ liệu bắt buộc và chuẩn hóa.
3. Tách dữ liệu hợp lệ khỏi dữ liệu lỗi.
4. Nạp dữ liệu hợp lệ vào PostgreSQL.
5. Tính số đơn và doanh thu theo ngày.
6. Chịu được consumer restart và event được giao lại.
7. Có khả năng replay để xây lại bảng tổng hợp.
8. Theo dõi lag, lỗi và DLQ.

```mermaid
flowchart LR
    G["Python order generator"] --> R["orders.raw.v1"]
    R --> V["Python validator"]
    V -->|Hợp lệ| C["orders.validated.v1"]
    V -->|Không hợp lệ| D["orders.dlq.v1"]
    C --> S["PostgreSQL sink"]
    S --> P["orders và daily_order_metrics"]
    C --> T["orders.state.v1 - compacted"]
    D --> X["Kiểm tra, sửa và replay"]
    X --> R
```

### Vì sao study case này phù hợp với Data Engineer?

- **Raw topic** tương đương vùng landing/bronze.
- **Validated topic** tương đương vùng dữ liệu đã làm sạch/silver.
- **PostgreSQL aggregate** tương đương data mart/gold thu nhỏ.
- **DLQ** mô phỏng quy trình quản trị data quality.
- **Replay** là kỹ năng quan trọng khi sửa logic hoặc xây lại projection.
- **Consumer lag** thể hiện độ trễ của pipeline.
- **Partition và consumer group** cho thấy cách scale ingestion/processing.

### Event mẫu

```json
{
  "event_id": "f4fe45cb-ef94-4317-aa72-f5363983a6df",
  "event_type": "OrderCreated",
  "event_version": 1,
  "occurred_at": "2026-07-30T08:30:00+00:00",
  "order_id": "ORD-1001",
  "customer_id": "CUS-101",
  "amount": 750000,
  "currency": "VND"
}
```

| Field | Lý do tồn tại |
|---|---|
| `event_id` | Nhận diện một lần phát event và chống xử lý trùng |
| `event_type` | Phân biệt loại sự kiện |
| `event_version` | Hỗ trợ thay đổi schema |
| `occurred_at` | Event time, luôn có timezone |
| `order_id` | Business identity và Kafka message key |
| `customer_id` | Dimension tối thiểu cho phân tích |
| `amount` | Measure doanh thu |
| `currency` | Không để giá trị tiền tệ mất đơn vị |

### Topic catalog

| Topic | Partition | Cleanup policy | Mục đích |
|---|---:|---|---|
| `orders.raw.v1` | 3 | `delete` | Event gốc từ nguồn |
| `orders.validated.v1` | 3 | `delete` | Event đã qua kiểm tra |
| `orders.dlq.v1` | 3 | `delete` | Event không thể xử lý |
| `orders.state.v1` | 3 | `compact` | Trạng thái mới nhất theo `order_id` |

Local chỉ có một broker nên replication factor là `1`. Đây là lựa chọn để học,
không phải cấu hình production.

---

## 4. Roadmap tổng thể

Thời lượng gợi ý: **10 tuần**, mỗi tuần 5 buổi, mỗi buổi 60-90 phút.

| Stage | Tuần | Trọng tâm | Sản phẩm kiểm chứng |
|---:|---:|---|---|
| 0 | 1 | Mental model và Docker/KRaft | Kafka chạy, describe được topic |
| 1 | 1 | CLI, key, partition, offset | Tự produce/consume và đọc metadata |
| 2 | 2 | Python producer | Generator phát order có delivery report |
| 3 | 3 | Python consumer và offset | Consumer manual commit, restart không mất dữ liệu |
| 4 | 4 | Data quality pipeline | Raw -> validated hoặc DLQ |
| 5 | 5 | Consumer group, scale, rebalance | Chứng minh giới hạn song song theo partition |
| 6 | 6 | Reliability, retry, idempotency | Duplicate không làm sai kết quả |
| 7 | 7 | PostgreSQL sink và Outbox | At-least-once + idempotent database write |
| 8 | 8 | Retention, compaction và replay | Rebuild projection bằng group mới |
| 9 | 9 | Kafka transaction và stream aggregation | Hiểu đúng phạm vi exactly-once |
| 10 | 10 | Monitoring, security, production | Runbook và bài kiểm tra cuối khóa |

### Vòng lặp học cho mọi stage

1. **Đọc:** chỉ đọc phần lý thuyết cần dùng.
2. **Dự đoán:** ghi topic, key, partition, group và offset dự kiến.
3. **Chạy:** thực hiện ví dụ nhỏ nhất.
4. **Quan sát:** xem output, CLI, lag và database.
5. **Phá:** dừng consumer, gửi duplicate hoặc gửi dữ liệu lỗi.
6. **Giải thích:** viết lại điều đã xảy ra bằng lời của bạn.
7. **Sửa:** thêm reliability phù hợp.
8. **Chạy lại:** dùng đúng kịch bản lỗi cũ để so sánh.

Mẫu nhật ký:

```text
Ngày:
Stage:
Khái niệm:

Tôi dự đoán:
- Topic:
- Key:
- Partition:
- Consumer group:
- Offset trước/sau:
- Điều xảy ra nếu process dừng:

Tôi quan sát:

Khác với dự đoán:

Tôi có thể tự giải thích:

Câu hỏi còn lại:
```

---

## 5. Bộ khung môi trường dùng cho toàn lộ trình

Tạo thư mục:

```text
kafka-de-learning/
├── compose.yaml
├── Dockerfile
├── requirements.txt
├── sql/
│   └── init.sql
└── src/
    ├── order_producer.py
    ├── order_consumer.py
    ├── validator.py
    ├── postgres_sink.py
    ├── replay_dlq.py
    └── transactional_transformer.py
```

### `compose.yaml`

```yaml
name: kafka-de-learning

services:
  broker:
    image: apache/kafka:4.3.1
    hostname: broker
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@broker:29093
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: >-
        CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_LISTENERS: >-
        CONTROLLER://:29093,PLAINTEXT://:19092,PLAINTEXT_HOST://:9092
      KAFKA_ADVERTISED_LISTENERS: >-
        PLAINTEXT://broker:19092,PLAINTEXT_HOST://localhost:9092
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      CLUSTER_ID: 4L6g3nShT-eMCtK--X86sw
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
      KAFKA_LOG_DIRS: /tmp/kraft-combined-logs
    volumes:
      - kafka_data:/tmp/kraft-combined-logs

  postgres:
    image: postgres:18.4
    environment:
      POSTGRES_USER: kafka_user
      POSTGRES_PASSWORD: kafka_password
      POSTGRES_DB: kafka_learning
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U kafka_user -d kafka_learning"]
      interval: 5s
      timeout: 3s
      retries: 10
    volumes:
      - postgres_data:/var/lib/postgresql
      - ./sql/init.sql:/docker-entrypoint-initdb.d/init.sql:ro

  app:
    build: .
    working_dir: /app
    volumes:
      - .:/app
    environment:
      KAFKA_BOOTSTRAP_SERVERS: broker:19092
      POSTGRES_DSN: >-
        postgresql://kafka_user:kafka_password@postgres:5432/kafka_learning
    command: ["sleep", "infinity"]
    depends_on:
      - broker
      - postgres

volumes:
  kafka_data:
  postgres_data:
```

Vì sao có hai Kafka listener?

- Python chạy trong Docker dùng `broker:19092`.
- CLI hoặc Python chạy trực tiếp trên máy dùng `localhost:9092`.
- Broker phải quảng bá địa chỉ mà client thực sự truy cập được. Sai
  `advertised.listeners` là một lỗi local rất phổ biến.

### `Dockerfile`

```dockerfile
FROM python:3.14.6-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["sleep", "infinity"]
```

### `requirements.txt`

```text
confluent-kafka==2.15.0
psycopg[binary]==3.3.4
```

### `sql/init.sql`

```sql
CREATE TABLE IF NOT EXISTS orders (
    order_id TEXT PRIMARY KEY,
    event_id UUID NOT NULL UNIQUE,
    customer_id TEXT NOT NULL,
    amount NUMERIC(18, 2) NOT NULL CHECK (amount >= 0),
    currency CHAR(3) NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS processed_events (
    consumer_group TEXT NOT NULL,
    event_id UUID NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (consumer_group, event_id)
);

CREATE TABLE IF NOT EXISTS daily_order_metrics (
    metric_date DATE PRIMARY KEY,
    order_count BIGINT NOT NULL,
    revenue NUMERIC(18, 2) NOT NULL
);

CREATE TABLE IF NOT EXISTS outbox_events (
    id UUID PRIMARY KEY,
    aggregate_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ
);
```

`processed_events` dùng khóa chính `(consumer_group, event_id)` thay vì chỉ
`event_id`, vì nhiều consumer group độc lập có quyền xử lý cùng một event.

Khởi động:

```bash
docker compose up -d --build
docker compose ps
docker compose logs broker --tail 30
```

Không chạy `docker compose down -v` trừ khi bạn cố ý muốn xóa toàn bộ dữ liệu
Kafka và PostgreSQL của lab.

---

## Stage 0 - Kafka mental model và KRaft

### Mục tiêu

- Chạy được Kafka trong Docker.
- Phân biệt broker, controller, topic, partition và record.
- Hiểu local cluster một node không có high availability.

### Đọc trước

- *Kafka: The Definitive Guide*: Chapter 1 và phần broker configuration của
  Chapter 2.
- *DDIA*: Chapter 1.

### Thực hành

Tạo bốn topic:

```bash
docker compose exec broker /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server broker:19092 \
  --create \
  --topic orders.raw.v1 \
  --partitions 3 \
  --replication-factor 1

docker compose exec broker /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server broker:19092 \
  --create \
  --topic orders.validated.v1 \
  --partitions 3 \
  --replication-factor 1

docker compose exec broker /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server broker:19092 \
  --create \
  --topic orders.dlq.v1 \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=2592000000

docker compose exec broker /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server broker:19092 \
  --create \
  --topic orders.state.v1 \
  --partitions 3 \
  --replication-factor 1 \
  --config cleanup.policy=compact
```

Liệt kê và kiểm tra:

```bash
docker compose exec broker /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server broker:19092 \
  --list

docker compose exec broker /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server broker:19092 \
  --describe \
  --topic orders.raw.v1
```

### Output mẫu

```text
Topic: orders.raw.v1  PartitionCount: 3  ReplicationFactor: 1
Topic: orders.raw.v1  Partition: 0  Leader: 1  Replicas: 1  Isr: 1
Topic: orders.raw.v1  Partition: 1  Leader: 1  Replicas: 1  Isr: 1
Topic: orders.raw.v1  Partition: 2  Leader: 1  Replicas: 1  Isr: 1
```

### Vì sao kết quả như vậy?

- Topic có ba partition nên có ba log độc lập.
- Mỗi partition có offset riêng bắt đầu từ 0.
- `Leader: 1` vì chỉ có broker mang node ID 1.
- Replication factor 1 nghĩa là không có bản sao để failover.
- KRaft lưu metadata bằng controller quorum, không cần ZooKeeper.

### Thí nghiệm

1. Dừng broker: `docker compose stop broker`.
2. Thử describe topic và ghi nhận lỗi.
3. Chạy lại broker: `docker compose start broker`.
4. Describe lại và xác nhận topic vẫn còn do named volume.

### Hoàn thành khi

- Bạn tự vẽ được producer -> broker -> topic -> partition -> consumer.
- Bạn giải thích được vì sao ba partition không đồng nghĩa ba bản sao.
- Bạn biết `down` khác `down -v`.

---

## Stage 1 - Produce và consume bằng Kafka CLI

### Mục tiêu

Nhìn trực tiếp key, value, partition và offset trước khi Python che bớt chi tiết.

### Producer input

```bash
docker compose exec broker /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server broker:19092 \
  --topic orders.raw.v1 \
  --property parse.key=true \
  --property key.separator=:
```

Nhập ba dòng:

```text
ORD-1001:{"event_id":"00000000-0000-0000-0000-000000000001","event_type":"OrderCreated","event_version":1,"occurred_at":"2026-07-30T08:30:00+00:00","order_id":"ORD-1001","customer_id":"CUS-101","amount":750000,"currency":"VND"}
ORD-1002:{"event_id":"00000000-0000-0000-0000-000000000002","event_type":"OrderCreated","event_version":1,"occurred_at":"2026-07-30T08:31:00+00:00","order_id":"ORD-1002","customer_id":"CUS-102","amount":320000,"currency":"VND"}
ORD-1001:{"event_id":"00000000-0000-0000-0000-000000000003","event_type":"OrderUpdated","event_version":1,"occurred_at":"2026-07-30T08:32:00+00:00","order_id":"ORD-1001","customer_id":"CUS-101","amount":800000,"currency":"VND"}
```

Mở consumer ở terminal khác:

```bash
docker compose exec broker /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server broker:19092 \
  --topic orders.raw.v1 \
  --from-beginning \
  --property print.key=true \
  --property print.partition=true \
  --property print.offset=true
```

### Output mẫu

Thứ tự cột có thể khác nhẹ theo formatter, nhưng metadata sẽ tương tự:

```text
Partition:1  Offset:0  ORD-1001  {"event_id":"...0001",...}
Partition:2  Offset:0  ORD-1002  {"event_id":"...0002",...}
Partition:1  Offset:1  ORD-1001  {"event_id":"...0003",...}
```

Partition cụ thể của mỗi key không cần giống output mẫu. Điều bắt buộc là trong
cùng topic và khi số partition không đổi, hai record có cùng key `ORD-1001`
được đưa vào cùng partition và có offset tăng dần tại partition đó.

### Vì sao dùng `order_id` làm key?

- Các thay đổi của cùng một order cần ordering trong `orders.raw.v1`.
- Kafka chỉ đảm bảo ordering trong một partition.
- Dùng `event_id` làm key sẽ làm các event của cùng order có thể nằm ở nhiều
  partition.
- Không có key làm producer phân phối record mà không giữ grouping theo order.

### Consumer group

Chạy hai consumer ở hai terminal với cùng group:

```bash
docker compose exec broker /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server broker:19092 \
  --topic orders.raw.v1 \
  --group cli-lab \
  --property print.partition=true \
  --property print.offset=true
```

Gửi thêm 20 record và kiểm tra group:

```bash
docker compose exec broker /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server broker:19092 \
  --describe \
  --group cli-lab
```

### Output mẫu

```text
GROUP    TOPIC          PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
cli-lab  orders.raw.v1  0          8               8               0
cli-lab  orders.raw.v1  1          7               7               0
cli-lab  orders.raw.v1  2          8               8               0
```

`LAG = LOG-END-OFFSET - CURRENT-OFFSET`. Lag bằng 0 chỉ nói group đã theo kịp
log; nó không chứng minh dữ liệu nghiệp vụ đúng.

### Hoàn thành khi

- Bạn phân biệt được key, partition và offset.
- Bạn chứng minh hai group khác nhau có thể đọc cùng một record độc lập.
- Bạn giải thích được đọc record không làm record biến mất khỏi Kafka.

---

## Stage 2 - Python producer đầu tiên

### Mục tiêu

Viết một generator phát order hợp lệ, dùng `order_id` làm key và kiểm tra
delivery report từ broker.

### `src/order_producer.py`

```python
import json
import os
import random
import time
import uuid
from datetime import UTC, datetime

from confluent_kafka import Producer


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:19092")
TOPIC = "orders.raw.v1"


def delivery_report(error, message):
    if error is not None:
        print(f"FAILED: {error}")
        return

    print(
        "DELIVERED "
        f"topic={message.topic()} "
        f"partition={message.partition()} "
        f"offset={message.offset()} "
        f"key={message.key().decode('utf-8')}"
    )


def make_order(number):
    order_id = f"ORD-{number:04d}"
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "OrderCreated",
        "event_version": 1,
        "occurred_at": datetime.now(UTC).isoformat(),
        "order_id": order_id,
        "customer_id": f"CUS-{random.randint(100, 105)}",
        "amount": random.choice([120000, 250000, 500000, 750000]),
        "currency": "VND",
    }


def main():
    producer = Producer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "client.id": "order-generator",
            "acks": "all",
            "enable.idempotence": True,
        }
    )

    for number in range(1, 11):
        event = make_order(number)
        value = json.dumps(event).encode("utf-8")
        key = event["order_id"].encode("utf-8")

        producer.produce(
            topic=TOPIC,
            key=key,
            value=value,
            on_delivery=delivery_report,
        )

        # Chạy callback của các lần gửi đã hoàn tất.
        producer.poll(0)
        time.sleep(0.2)

    remaining = producer.flush(10)
    if remaining:
        raise RuntimeError(f"Con {remaining} message chua gui xong")


if __name__ == "__main__":
    main()
```

Chạy:

```bash
docker compose exec app python src/order_producer.py
```

### Output mẫu

```text
DELIVERED topic=orders.raw.v1 partition=2 offset=3 key=ORD-0001
DELIVERED topic=orders.raw.v1 partition=0 offset=2 key=ORD-0002
DELIVERED topic=orders.raw.v1 partition=1 offset=4 key=ORD-0003
...
```

### Vì sao code như vậy?

- `produce()` là bất đồng bộ; method trả về không có nghĩa broker đã ghi xong.
- Delivery callback được chạy khi gọi `poll()` hoặc `flush()`.
- `flush()` trước khi process kết thúc để không bỏ lại message trong local
  producer queue.
- `acks=all` yêu cầu leader chờ tất cả in-sync replicas xác nhận. Local có một
  replica nên chưa kiểm chứng được failover.
- `enable.idempotence=true` chống duplicate do producer retry trong một producer
  session; nó không chống việc business code gọi `produce()` hai lần.
- JSON được encode thành bytes vì Kafka lưu byte arrays, không hiểu field JSON.

### Thí nghiệm

1. Dừng Kafka rồi chạy producer.
2. Quan sát producer chờ hoặc báo lỗi thay vì giả vờ thành công.
3. Khởi động Kafka và chạy lại.
4. Sửa code để gửi 20 event cùng `order_id`, xác nhận cùng partition.
5. Bỏ key và so sánh phân bố.

### Hoàn thành khi

- Mọi lần gửi đều có delivery report.
- Bạn giải thích được vì sao cần `poll()`/`flush()`.
- Bạn không nhầm broker acknowledgment với consumer đã xử lý thành công.

---

## Stage 3 - Python consumer, offset và at-least-once

### Mục tiêu

- Đọc event bằng consumer group.
- Tắt auto commit.
- Commit offset sau khi xử lý thành công.
- Quan sát duplicate window khi crash.

### `src/order_consumer.py`

```python
import json
import os
import signal

from confluent_kafka import Consumer, KafkaError


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:19092")
TOPIC = "orders.raw.v1"
running = True


def stop(_signal_number, _frame):
    global running
    running = False


def main():
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": "raw-order-printer-v1",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([TOPIC])

    try:
        while running:
            message = consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(message.error())

            event = json.loads(message.value().decode("utf-8"))

            print(
                f"PROCESS order_id={event['order_id']} "
                f"partition={message.partition()} "
                f"offset={message.offset()}"
            )

            # Chỉ commit sau khi business processing đã thành công.
            consumer.commit(message=message, asynchronous=False)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
```

Chạy:

```bash
docker compose exec app python src/order_consumer.py
```

### Input mẫu

```json
{
  "event_id": "4a6440ef-24e1-4df6-af17-c31e46d7f181",
  "event_type": "OrderCreated",
  "event_version": 1,
  "occurred_at": "2026-07-30T09:00:00+00:00",
  "order_id": "ORD-0100",
  "customer_id": "CUS-101",
  "amount": 250000,
  "currency": "VND"
}
```

### Output mẫu

```text
PROCESS order_id=ORD-0100 partition=1 offset=15
```

Committed offset sẽ là vị trí tiếp theo cần đọc, về mặt khái niệm là `16`, không
phải record cuối cùng là `15`.

### Vì sao commit sau xử lý?

| Thứ tự | Nếu process crash | Kết quả |
|---|---|---|
| Commit rồi mới xử lý | Offset đã đi tiếp nhưng output chưa có | Có thể mất xử lý |
| Xử lý rồi mới commit | Output có nhưng offset chưa đi tiếp | Record có thể được giao lại |

Lộ trình chọn **at-least-once**: chấp nhận khả năng duplicate rồi làm consumer
idempotent ở Stage 6-7. Đây thường là mặc định an toàn hơn cho data pipeline.

`auto.offset.reset=earliest` chỉ có tác dụng khi group chưa có committed offset
hợp lệ. Nó không khiến một group cũ luôn đọc lại từ đầu.

### Crash lab

Tạm thêm dòng:

```python
raise RuntimeError("Gia lap crash sau khi xu ly, truoc commit")
```

đặt ngay trước `consumer.commit(...)`.

Expected output sau mỗi lần restart:

```text
PROCESS order_id=ORD-0100 partition=1 offset=15
Traceback ... RuntimeError: Gia lap crash ...
```

Record offset 15 xuất hiện lại vì chưa được commit.

### Hoàn thành khi

- Bạn phân biệt current position và committed offset.
- Bạn tái hiện được duplicate delivery.
- Bạn giải thích được tại sao manual commit chưa tự tạo exactly-once.

---

## Stage 4 - Data quality: raw -> validated hoặc DLQ

### Mục tiêu

Xây bước transform đầu tiên của pipeline:

```text
orders.raw.v1
-> validate + normalize
-> orders.validated.v1 hoặc orders.dlq.v1
```

### Quy tắc dữ liệu

Event hợp lệ khi:

- Có đủ các field bắt buộc.
- `event_type == "OrderCreated"`.
- `event_version == 1`.
- `event_id` là UUID hợp lệ.
- `order_id` và `customer_id` là string không rỗng.
- Kafka message key bằng `order_id`.
- `amount` là số không âm.
- `currency == "VND"`.
- `occurred_at` parse được và có timezone.

### `src/validator.py`

```python
import json
import os
from datetime import datetime
from uuid import UUID

from confluent_kafka import Consumer, Producer


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:19092")
INPUT_TOPIC = "orders.raw.v1"
OUTPUT_TOPIC = "orders.validated.v1"
DLQ_TOPIC = "orders.dlq.v1"
REQUIRED_FIELDS = {
    "event_id",
    "event_type",
    "event_version",
    "occurred_at",
    "order_id",
    "customer_id",
    "amount",
    "currency",
}


def validate(event, message_key):
    missing = REQUIRED_FIELDS - event.keys()
    if missing:
        raise ValueError(f"missing_fields={sorted(missing)}")
    if event["event_type"] != "OrderCreated":
        raise ValueError("unsupported_event_type")
    if event["event_version"] != 1:
        raise ValueError("unsupported_event_version")
    UUID(event["event_id"])
    if not isinstance(event["order_id"], str) or not event["order_id"].strip():
        raise ValueError("invalid_order_id")
    if not isinstance(event["customer_id"], str) or not event["customer_id"].strip():
        raise ValueError("invalid_customer_id")
    if message_key != event["order_id"]:
        raise ValueError("message_key_must_equal_order_id")
    if not isinstance(event["amount"], (int, float)) or event["amount"] < 0:
        raise ValueError("invalid_amount")
    if event["currency"] != "VND":
        raise ValueError("unsupported_currency")

    occurred_at = datetime.fromisoformat(event["occurred_at"])
    if occurred_at.tzinfo is None:
        raise ValueError("occurred_at_requires_timezone")


def main():
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": "order-validator-v1",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    producer = Producer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "enable.idempotence": True,
            "acks": "all",
        }
    )
    consumer.subscribe([INPUT_TOPIC])

    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                raise RuntimeError(message.error())

            raw_text = message.value().decode("utf-8", errors="replace")
            message_key = (
                message.key().decode("utf-8") if message.key() else None
            )

            try:
                event = json.loads(raw_text)
                validate(event, message_key)
                target_topic = OUTPUT_TOPIC
                output = event
                result = "VALID"
            except (json.JSONDecodeError, ValueError, TypeError) as error:
                target_topic = DLQ_TOPIC
                output = {
                    "original_topic": message.topic(),
                    "original_partition": message.partition(),
                    "original_offset": message.offset(),
                    "original_key": message_key,
                    "raw_value": raw_text,
                    "error_type": type(error).__name__,
                    "error_message": str(error),
                }
                result = "DLQ"

            producer.produce(
                topic=target_topic,
                key=message.key(),
                value=json.dumps(output).encode("utf-8"),
            )

            # Stage này chọn cách dễ hiểu: đợi output được broker xác nhận.
            if producer.flush(10) != 0:
                raise RuntimeError("Output event chua gui xong")

            consumer.commit(message=message, asynchronous=False)
            print(
                f"{result} source_partition={message.partition()} "
                f"source_offset={message.offset()}"
            )
    finally:
        producer.flush(10)
        consumer.close()


if __name__ == "__main__":
    main()
```

### Input hợp lệ

```json
{"event_id":"4a6440ef-24e1-4df6-af17-c31e46d7f181","event_type":"OrderCreated","event_version":1,"occurred_at":"2026-07-30T09:00:00+00:00","order_id":"ORD-0100","customer_id":"CUS-101","amount":250000,"currency":"VND"}
```

Output:

```text
VALID source_partition=1 source_offset=15
```

và payload xuất hiện ở `orders.validated.v1`.

### Input không hợp lệ

```json
{"event_id":"bad-1","event_type":"OrderCreated","order_id":"ORD-0101","amount":-1}
```

Output:

```text
DLQ source_partition=0 source_offset=9
```

DLQ payload mẫu:

```json
{
  "original_topic": "orders.raw.v1",
  "original_partition": 0,
  "original_offset": 9,
  "original_key": "ORD-0101",
  "raw_value": "{\"event_id\":\"bad-1\",...}",
  "error_type": "ValueError",
  "error_message": "missing_fields=['customer_id', 'currency', 'event_version', 'occurred_at']"
}
```

### Vì sao DLQ phải giữ metadata gốc?

- Topic/partition/offset xác định chính xác nguồn của record.
- Key giúp bảo toàn business identity khi replay.
- Raw value giúp điều tra dữ liệu thực tế đã nhận.
- Error type/message giúp phân nhóm lỗi data quality.
- Không đưa password, token hoặc dữ liệu nhạy cảm vào error payload.

### Giới hạn quan trọng của code Stage 4

Output topic và input offset chưa nằm trong cùng transaction. Có hai cửa sổ:

1. Output đã ghi, process chết trước input commit -> output có thể bị ghi trùng.
2. Nếu commit input trước output -> có thể mất output.

Stage này cố ý dùng at-least-once và sẽ thêm idempotency. Kafka transaction được
học ở Stage 9.

### Hoàn thành khi

- JSON hỏng không làm consumer kẹt vô hạn.
- Business rule lỗi đi DLQ với metadata đủ điều tra.
- Input hợp lệ luôn tới topic validated.
- Bạn chỉ ra được duplicate window của pipeline.

---

## Stage 5 - Partition, consumer group, scale và rebalance

### Mục tiêu

Chứng minh khả năng song song bị giới hạn bởi số partition.

### Thực hành

Mở nhiều terminal và chạy cùng consumer:

```bash
docker compose run --rm app python src/order_consumer.py
```

Lần lượt chạy:

1. Một instance.
2. Hai instance cùng `group.id`.
3. Ba instance.
4. Bốn instance.

Describe group:

```bash
docker compose exec broker /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server broker:19092 \
  --describe \
  --group raw-order-printer-v1
```

### Kết quả mong đợi

| Partition | Consumer instance khi có 3 consumer |
|---:|---|
| 0 | Consumer A |
| 1 | Consumer B |
| 2 | Consumer C |

Consumer thứ tư không có partition nên idle. Khi dừng Consumer B, group
rebalance và partition 1 được gán lại cho A hoặc C.

### Vì sao?

- Trong một group, tại một thời điểm một partition chỉ được xử lý bởi một
  consumer.
- Một consumer có thể giữ nhiều partition.
- Số consumer hữu ích tối đa thường bằng số partition được subscribe.
- Hai group khác nhau vẫn đọc cùng dữ liệu độc lập.

### Ordering lab

1. Gửi 100 record cùng key `ORD-HOT`.
2. Xác nhận chúng vào cùng partition và offset tăng dần.
3. Gửi 100 record với nhiều key.
4. So sánh phân bố.
5. Gửi 90% record bằng key `VIP-CUSTOMER`.

Expected:

- `ORD-HOT` giữ ordering nhưng tạo hot partition.
- Nhiều key giúp phân bố tốt hơn.
- Thêm consumer không giải quyết hot partition nếu mọi dữ liệu nóng có cùng key.

### Bài học thiết kế

Chọn key là một quyết định nghiệp vụ:

- Key quá chi tiết: phân bố tốt nhưng mất ordering cần thiết.
- Key quá rộng: giữ ordering nhưng tạo bottleneck.
- Với order event, `order_id` là điểm cân bằng hợp lý.
- Tăng số partition có thể làm mapping của key cho record tương lai thay đổi.

### Hoàn thành khi

- Bạn dự đoán đúng assignment với 1-4 consumer.
- Bạn mô tả rebalance mà không dùng câu “Kafka tự chia đại”.
- Bạn giải thích được ordering chỉ tồn tại trong partition, không phải toàn topic.

---

## Stage 6 - Reliability, retry, DLQ và idempotency

### Mục tiêu

Thiết kế pipeline chịu được transient error, poison message, duplicate và
consumer restart.

### Phân loại lỗi trước khi retry

| Loại | Ví dụ | Cách xử lý |
|---|---|---|
| Business outcome | Đơn có amount bằng 0 theo policy | Ghi trạng thái hợp lệ, không retry kỹ thuật |
| Transient technical | DB timeout, network ngắt ngắn | Retry hữu hạn với backoff |
| Permanent data error | JSON hỏng, thiếu field | DLQ ngay hoặc sau rất ít lần |
| Programming bug | `KeyError` ngoài dự kiến | Retry ít lần, DLQ và alert |
| Dependency down lâu | PostgreSQL dừng | Backoff/pause, tránh retry storm |

### Retry đơn giản cho người mới

```python
import time


def run_with_retry(operation, max_attempts=3):
    for attempt in range(1, max_attempts + 1):
        try:
            return operation()
        except TimeoutError:
            if attempt == max_attempts:
                raise
            delay_seconds = 2 ** (attempt - 1)
            print(f"RETRY attempt={attempt} sleep={delay_seconds}s")
            time.sleep(delay_seconds)
```

Input giả lập:

```text
attempt 1 -> TimeoutError
attempt 2 -> TimeoutError
attempt 3 -> success
```

Output:

```text
RETRY attempt=1 sleep=1s
RETRY attempt=2 sleep=2s
SUCCESS
```

Backoff làm giảm áp lực lên dependency đang lỗi. Không retry vô hạn vì một
record có thể chặn cả partition.

### Blocking và non-blocking retry

**Blocking retry** giữ record trong consumer process:

- Dễ hiểu và giữ ordering của partition.
- Record lỗi chặn tất cả record phía sau cùng partition.

**Non-blocking retry** publish sang retry topic:

- Partition gốc tiếp tục chạy.
- Cần thêm topic, retry timestamp và scheduler.
- Có thể phá ordering.

Người mới nên dùng blocking retry với số lần nhỏ trước, sau đó mới thử retry
topic.

### Idempotency rule

Cùng một `event_id` có thể được Kafka giao lại. Kết quả nghiệp vụ phải giống như
xử lý một lần.

Không deduplicate bằng Python `set()` trong memory vì:

- Mất dữ liệu khi restart.
- Không chia sẻ giữa nhiều consumer instance.
- Có thể tăng memory không giới hạn.

Dùng database unique constraint ở Stage 7.

### DLQ replay checklist

Trước khi replay:

1. Root cause đã được sửa chưa?
2. Payload có cần migration không?
3. Consumer đích có idempotent không?
4. Replay vào topic gốc hay recovery topic?
5. Có rate limit không?
6. Có ghi ai replay, khi nào, bao nhiêu record không?

### `src/replay_dlq.py`

Script tối giản sau replay nhiều nhất 10 record. Chỉ chạy sau khi nguyên nhân làm
validator từ chối dữ liệu đã được sửa. Nếu payload cần migration, hãy transform
payload có kiểm soát trước khi produce lại; không replay nguyên trạng rồi hy vọng
kết quả khác đi.

```python
import json
import os

from confluent_kafka import Consumer, Producer


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:19092")
MAX_RECORDS = 10


def main():
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": "dlq-replayer-lab-v1",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    producer = Producer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "enable.idempotence": True,
            "acks": "all",
        }
    )
    consumer.subscribe(["orders.dlq.v1"])

    replayed = 0
    try:
        while replayed < MAX_RECORDS:
            message = consumer.poll(2.0)
            if message is None:
                break
            if message.error():
                raise RuntimeError(message.error())

            dlq_event = json.loads(message.value().decode("utf-8"))
            original_key = dlq_event["original_key"]
            raw_value = dlq_event["raw_value"]

            if original_key is None:
                print("STOPPED reason=record_can_sua_key_truoc_khi_replay")
                break

            producer.produce(
                "orders.raw.v1",
                key=original_key.encode("utf-8"),
                value=raw_value.encode("utf-8"),
            )
            if producer.flush(10) != 0:
                raise RuntimeError("Replay event chua gui xong")

            consumer.commit(message=message, asynchronous=False)
            replayed += 1
            print(
                f"REPLAYED original_topic={dlq_event['original_topic']} "
                f"original_partition={dlq_event['original_partition']} "
                f"original_offset={dlq_event['original_offset']}"
            )
    finally:
        producer.flush(10)
        consumer.close()

    print(f"DONE replayed={replayed}")


if __name__ == "__main__":
    main()
```

Output mẫu:

```text
REPLAYED original_topic=orders.raw.v1 original_partition=0 original_offset=9
DONE replayed=1
```

Đây là tool học tập, chưa phải production replayer. Production cần approval,
audit, rate limit, chọn record cụ thể, dry run và cơ chế ngăn một record chạy
vòng lặp raw -> DLQ -> replay vô hạn.

### Thí nghiệm

- Gửi cùng payload và cùng `event_id` năm lần.
- Gửi cùng payload nhưng năm `event_id` khác nhau.
- Làm database timeout hai lần rồi hồi phục.
- Gửi JSON hỏng.
- Dừng dependency lâu hơn tổng retry.
- Replay cùng DLQ record hai lần.

Thí nghiệm thứ hai chứng minh `event_id` deduplication không thể thay thế
business invariant. Với `OrderCreated`, Stage 7 còn dùng unique `order_id` để
không tạo và không cộng doanh thu cho cùng một order lần nữa.

### Hoàn thành khi

- Consumer không retry vô hạn.
- Business outcome không bị đưa nhầm vào DLQ.
- Bạn phân biệt idempotent producer và idempotent consumer.
- Bạn giải thích được trade-off ordering của retry topic.

---

## Stage 7 - PostgreSQL sink: at-least-once nhưng không cộng trùng

### Mục tiêu

Consume `orders.validated.v1`, ghi order và cập nhật daily metrics trong một
database transaction, đồng thời chống duplicate bằng `processed_events`.

### `src/postgres_sink.py`

```python
import json
import os
import signal
from datetime import datetime
from decimal import Decimal

import psycopg
from confluent_kafka import Consumer


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:19092")
POSTGRES_DSN = os.environ["POSTGRES_DSN"]
GROUP_ID = "postgres-order-sink-v1"
TOPIC = "orders.validated.v1"
running = True


def stop(_signal_number, _frame):
    global running
    running = False


def store_event(connection, event):
    occurred_at = datetime.fromisoformat(event["occurred_at"])
    metric_date = occurred_at.date()

    with connection.transaction():
        inserted = connection.execute(
            """
            INSERT INTO processed_events (consumer_group, event_id)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING
            RETURNING event_id
            """,
            (GROUP_ID, event["event_id"]),
        ).fetchone()

        if inserted is None:
            return "DUPLICATE"

        inserted_order = connection.execute(
            """
            INSERT INTO orders (
                order_id,
                event_id,
                customer_id,
                amount,
                currency,
                occurred_at
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (order_id) DO NOTHING
            RETURNING order_id
            """,
            (
                event["order_id"],
                event["event_id"],
                event["customer_id"],
                Decimal(str(event["amount"])),
                event["currency"],
                occurred_at,
            ),
        ).fetchone()

        # event_id mới nhưng order_id cũ vẫn là duplicate ở tầng nghiệp vụ.
        if inserted_order is None:
            return "DUPLICATE_ORDER"

        connection.execute(
            """
            INSERT INTO daily_order_metrics (
                metric_date,
                order_count,
                revenue
            )
            VALUES (%s, 1, %s)
            ON CONFLICT (metric_date)
            DO UPDATE SET
                order_count = daily_order_metrics.order_count + 1,
                revenue = daily_order_metrics.revenue + EXCLUDED.revenue
            """,
            (metric_date, Decimal(str(event["amount"]))),
        )

    return "STORED"


def main():
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([TOPIC])

    with psycopg.connect(POSTGRES_DSN) as connection:
        try:
            while running:
                message = consumer.poll(1.0)
                if message is None:
                    continue
                if message.error():
                    raise RuntimeError(message.error())

                event = json.loads(message.value().decode("utf-8"))
                result = store_event(connection, event)

                # DB đã commit. Sau đó mới commit Kafka offset.
                consumer.commit(message=message, asynchronous=False)
                print(
                    f"{result} event_id={event['event_id']} "
                    f"partition={message.partition()} "
                    f"offset={message.offset()}"
                )
        finally:
            consumer.close()


if __name__ == "__main__":
    main()
```

Chạy:

```bash
docker compose exec app python src/postgres_sink.py
```

Query kết quả:

```bash
docker compose exec postgres psql \
  -U kafka_user \
  -d kafka_learning \
  -c "SELECT * FROM daily_order_metrics ORDER BY metric_date;"
```

### Input mẫu

Hai lần cùng một event:

```json
{"event_id":"4a6440ef-24e1-4df6-af17-c31e46d7f181","event_type":"OrderCreated","event_version":1,"occurred_at":"2026-07-30T09:00:00+00:00","order_id":"ORD-0100","customer_id":"CUS-101","amount":250000,"currency":"VND"}
```

### Output ứng dụng

```text
STORED event_id=4a6440ef-24e1-4df6-af17-c31e46d7f181 partition=1 offset=15
DUPLICATE event_id=4a6440ef-24e1-4df6-af17-c31e46d7f181 partition=1 offset=16
DUPLICATE_ORDER event_id=71efb8f9-f877-4aa9-a78f-c48a00d88684 partition=1 offset=17
```

Dòng thứ ba là cùng `order_id` nhưng mang `event_id` mới.

### Output database

```text
 metric_date | order_count |  revenue
-------------+-------------+-----------
 2026-07-30  |           1 | 250000.00
```

### Vì sao không bị cộng doanh thu hai lần?

Trong cùng PostgreSQL transaction:

1. Insert `(consumer_group, event_id)`.
2. Unique constraint chỉ cho event mới đi tiếp.
3. Unique `order_id` chặn một lần tạo order khác mang `event_id` mới.
4. Chỉ khi order thực sự được insert mới update metric.
5. Commit tất cả hoặc rollback tất cả.

Sau đó mới commit Kafka offset. Nếu process chết sau DB commit nhưng trước Kafka
commit, record được giao lại; unique constraint phát hiện duplicate và không
cộng metric lần hai.

Đây là hai lớp idempotency khác nhau:

- `event_id` chống Kafka giao lại cùng một event.
- `order_id` bảo vệ invariant nghiệp vụ “một order chỉ được tạo một lần”.

### Phạm vi đảm bảo

Đây là **effectively-once business result** xây trên:

- Kafka at-least-once delivery.
- Database transaction.
- Durable idempotency key.
- Manual offset commit.

Nó không phải Kafka exactly-once transaction.

### Transactional Outbox

Khi một source service vừa ghi database vừa publish Kafka, code sau có lỗi
dual-write:

```text
1. INSERT order thành công
2. Process chết trước producer.produce()
3. Database có order nhưng Kafka không có event
```

Outbox giải quyết bằng một transaction:

```sql
BEGIN;

INSERT INTO orders (...);

INSERT INTO outbox_events (
    id,
    aggregate_id,
    event_type,
    payload
) VALUES (...);

COMMIT;
```

Một publisher riêng đọc row chưa publish, gửi Kafka, rồi đánh dấu
`published_at`. Publisher vẫn có thể gửi trùng nếu chết sau Kafka acknowledgment
nhưng trước update `published_at`; consumer idempotency vẫn cần thiết.

Với pipeline thực tế, sau khi hiểu polling outbox có thể học Kafka Connect và
Debezium CDC. Không thêm chúng sớm vì chúng che đi bài toán consistency cần hiểu.

### Hoàn thành khi

- Gửi cùng event năm lần nhưng metric chỉ tăng một lần.
- Crash sau DB commit, trước Kafka commit không làm sai kết quả.
- Bạn giải thích được Outbox ngăn mất event nhưng không ngăn duplicate tuyệt đối.

---

## Stage 8 - Retention, compaction, tombstone và replay

### Mục tiêu

Hiểu Kafka là một log có thể đọc lại và dùng log để xây lại projection.

### Retention

Tạo topic thử nghiệm có retention ngắn:

```bash
docker compose exec broker /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server broker:19092 \
  --create \
  --topic retention.lab.v1 \
  --partitions 1 \
  --replication-factor 1 \
  --config retention.ms=60000 \
  --config segment.ms=10000
```

Gửi record, chờ và quan sát. Record không nhất thiết biến mất chính xác sau
60 giây vì Kafka dọn log segment theo chu kỳ; retention không phải TTL chính xác
cho từng record.

### Compaction

Gửi trạng thái với key:

```text
ORD-1:{"order_id":"ORD-1","status":"CREATED"}
ORD-1:{"order_id":"ORD-1","status":"VALIDATED"}
ORD-1:{"order_id":"ORD-1","status":"LOADED"}
ORD-2:{"order_id":"ORD-2","status":"CREATED"}
```

Về lâu dài, `orders.state.v1` giữ ít nhất giá trị mới nhất của mỗi key:

```text
ORD-1 -> LOADED
ORD-2 -> CREATED
```

Compaction chạy nền và không xóa phiên bản cũ ngay lập tức.

Tombstone là record có key nhưng value `null`:

```text
key=ORD-2
value=null
```

Nó biểu diễn yêu cầu xóa key khỏi compacted state sau thời gian giữ tombstone.

### Replay bằng consumer group mới

Để xây lại projection:

1. Tạo bảng mới `orders_rebuild`.
2. Dùng group `postgres-order-rebuild-v1`.
3. Đặt `auto.offset.reset=earliest`.
4. Đọc `orders.validated.v1` từ đầu.
5. Không gửi email hoặc side effect bên ngoài khi replay.
6. So sánh bảng rebuild với bảng hiện tại.

Expected:

```text
original_orders_count = 1000
rebuild_orders_count  = 1000
status                = MATCH
```

### Reset offset

Reset offset có rủi ro. Quy trình an toàn:

1. Dừng toàn bộ consumer của group.
2. Ghi lại offset hiện tại.
3. Chọn đúng topic và partition.
4. Dry run.
5. Xác nhận sink idempotent hoặc ghi vào bảng mới.
6. Chỉ sau đó mới execute.

Ví dụ dry run:

```bash
docker compose exec broker /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server broker:19092 \
  --group postgres-order-sink-v1 \
  --topic orders.validated.v1 \
  --reset-offsets \
  --to-earliest \
  --dry-run
```

Không thêm `--execute` cho tới khi bạn đã đọc và hiểu kết quả dry run.

### Vì sao replay quan trọng với Data Engineer?

- Sửa bug transform rồi xây lại output.
- Thêm một data mart mới mà không gọi lại source.
- Khôi phục projection bị xóa.
- Backfill một consumer mới.
- Kiểm chứng dữ liệu nguồn và dữ liệu đích.

### Hoàn thành khi

- Bạn phân biệt delete retention và compaction.
- Bạn giải thích được compaction không xảy ra ngay.
- Bạn hiểu tombstone.
- Bạn rebuild được projection mà không tạo side effect ngoài ý muốn.

---

## Stage 9 - Kafka transaction và aggregation theo cửa sổ

Stage này gồm một phần bắt buộc về khái niệm và một lab nâng cao. Nếu Stage 0-8
chưa vững, chưa cần tối ưu exactly-once.

### 9.1 Kafka transaction: phạm vi đúng

Kafka transaction có thể làm atomic:

```text
đọc input Kafka
+ ghi output Kafka
+ commit input offset
```

Nó không tự động bao phủ:

- PostgreSQL transaction.
- REST API bên ngoài.
- Email/SMS.
- Object storage write.

### `src/transactional_transformer.py`

```python
import json
import os

from confluent_kafka import Consumer, Producer


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:19092")


def main():
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": "transactional-transformer-v1",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "isolation.level": "read_committed",
        }
    )
    producer = Producer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "transactional.id": "transactional-transformer-instance-1",
            "enable.idempotence": True,
        }
    )

    producer.init_transactions()
    consumer.subscribe(["orders.raw.v1"])

    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                raise RuntimeError(message.error())

            try:
                producer.begin_transaction()

                event = json.loads(message.value().decode("utf-8"))
                event["transactional_transform"] = True

                producer.produce(
                    "orders.validated.v1",
                    key=message.key(),
                    value=json.dumps(event).encode("utf-8"),
                )

                # position() trả về next offset cho các partition được gán.
                offsets = consumer.position(consumer.assignment())
                producer.send_offsets_to_transaction(
                    offsets,
                    consumer.consumer_group_metadata(),
                )
                producer.commit_transaction()

                print(
                    f"COMMITTED partition={message.partition()} "
                    f"offset={message.offset()}"
                )
            except Exception:
                producer.abort_transaction()
                raise
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
```

Output mẫu:

```text
COMMITTED partition=2 offset=41
```

Consumer output dùng `isolation.level=read_committed` sẽ không thấy record của
transaction đã abort.

### Vì sao `transactional.id` phải ổn định và duy nhất theo instance?

- Broker dùng nó để nhận diện transactional producer.
- Instance mới cùng identity có thể fence instance cũ.
- Hai instance sống đồng thời không nên vô tình dùng cùng ID.
- `init_transactions()` hoàn tất hoặc abort transaction còn dang dở trước đó.

### 9.2 Aggregation theo cửa sổ bằng Python

Kafka Streams là thư viện Java. Để giữ lộ trình Python đơn giản, phần bắt buộc
dùng consumer Python và PostgreSQL để nhóm theo event time. Mục tiêu là hiểu:

- Event time khác processing time.
- Window cần chính sách cho late event.
- Aggregation phải idempotent khi replay.

Ví dụ mapping:

```python
from datetime import datetime


def five_minute_window(occurred_at_text):
    timestamp = datetime.fromisoformat(occurred_at_text)
    minute = timestamp.minute - (timestamp.minute % 5)
    return timestamp.replace(minute=minute, second=0, microsecond=0)
```

Input:

```text
2026-07-30T09:02:10+00:00 amount=100000
2026-07-30T09:04:55+00:00 amount=250000
2026-07-30T09:05:01+00:00 amount=300000
```

Output:

```text
window_start=2026-07-30T09:00:00+00:00 count=2 revenue=350000
window_start=2026-07-30T09:05:00+00:00 count=1 revenue=300000
```

Late event:

```text
Đang xử lý window 09:10 nhưng nhận event có occurred_at=09:03.
```

Bạn phải chọn:

- Cập nhật lại window cũ.
- Chấp nhận trong một grace period.
- Đưa vào late-event topic.
- Bỏ qua có metric và lý do rõ ràng.

Không có lựa chọn đúng cho mọi hệ thống; quyết định phụ thuộc business SLA.

### Hướng mở rộng sau khóa học

Khi cần stateful stream processing production-grade:

- Kafka Streams nếu chấp nhận Java.
- Apache Flink hoặc Spark Structured Streaming cho pipeline phức tạp.
- Kafka Connect cho tích hợp source/sink chuẩn, không cho business logic lớn.

Không chọn framework mới trước khi hiểu key, partition, offset, state và
delivery semantics.

### Hoàn thành khi

- Bạn nói rõ “exactly-once trong phạm vi nào”.
- Bạn phân biệt aborted record và committed record.
- Bạn phân biệt event time và processing time.
- Bạn đưa ra policy rõ ràng cho late event.

---

## Stage 10 - Monitoring, security và production thinking

### Mục tiêu

Không coi “container đang Up” là bằng chứng pipeline khỏe.

### Log tối thiểu

Mỗi consumer log:

```text
service
instance_id
event_id
order_id
topic
partition
offset
consumer_group
processing_time_ms
result
retry_count
```

Không log password, token, thông tin thanh toán hoặc full payload nhạy cảm.

### Metric cần theo dõi

**Producer**

- Record send rate.
- Error rate.
- Retry rate.
- Request latency.
- Local queue size.

**Consumer**

- Record consume rate.
- Processing latency.
- Commit failure.
- Consumer lag theo group/topic/partition.
- Rebalance count và duration.
- Retry/DLQ rate.

**Data quality và nghiệp vụ**

- Valid/invalid event count.
- Invalid rate theo error type.
- Order count và revenue.
- Duplicate count.
- DLQ record count và tuổi record cũ nhất.
- End-to-end latency từ `occurred_at` tới `ingested_at`.

### Liveness, readiness và business health

| Loại | Câu hỏi |
|---|---|
| Liveness | Process có kẹt và cần restart không? |
| Readiness | Instance có sẵn sàng poll/xử lý không? |
| Business health | Event có đi hết pipeline trong SLO không? |

Lag bằng 0 nhưng dữ liệu đều bị validator đưa vào DLQ vẫn là pipeline hỏng về
nghiệp vụ.

### Runbook khi consumer lag tăng

1. Xác định group, topic và partition có lag.
2. Kiểm tra producer rate có tăng đột biến không.
3. Kiểm tra consumer error, retry, GC/process health.
4. Kiểm tra PostgreSQL latency và lock.
5. Tìm poison message hoặc hot partition.
6. So sánh số consumer với số partition.
7. Chỉ scale consumer nếu bottleneck thực sự ở consumer.
8. Không reset offset để “xóa lag” nếu chưa hiểu dữ liệu bị bỏ qua.

### Security

Local dùng PLAINTEXT để giảm độ khó. Production cần:

- TLS để mã hóa đường truyền.
- SASL hoặc cơ chế phù hợp để authentication.
- ACL theo least privilege.
- Secret manager thay vì password trong Compose.
- Network segmentation.
- Audit thao tác quản trị và replay.
- Schema/payload không chứa dữ liệu nhạy cảm không cần thiết.

Ví dụ quyền:

| Principal | Read | Write |
|---|---|---|
| order-generator | Không | `orders.raw.v1` |
| validator | `orders.raw.v1` | `orders.validated.v1`, `orders.dlq.v1` |
| postgres-sink | `orders.validated.v1` | Không |
| dlq-replayer | `orders.dlq.v1` | `orders.raw.v1` |

### Local so với production

| Local lab | Production cần cân nhắc |
|---|---|
| 1 broker/controller combined | Nhiều broker, controller quorum tách biệt |
| Replication factor 1 | Thường 3, tùy SLA |
| `min.insync.replicas=1` | Phối hợp với RF và `acks=all` |
| PLAINTEXT | TLS + authentication + ACL |
| Một máy | Rack/zone awareness |
| Named volume | Storage bền vững và capacity planning |
| CLI quan sát thủ công | Metrics, dashboard, alert, runbook |
| JSON không registry | Avro/Protobuf + Schema Registry khi cần governance |
| Tạo topic thủ công | Infrastructure as Code và review |

### Failure drill cuối stage

- Dừng một consumer khi đang có traffic.
- Dừng PostgreSQL hai phút.
- Dừng Kafka.
- Gửi duplicate.
- Gửi poison message.
- Tạo hot key.
- Replay bằng group mới.
- So sánh source count, validated count, DLQ count và database count.

### Hoàn thành khi

- Có cảnh báo lag, DLQ rate và end-to-end latency.
- Có runbook xử lý lag.
- Bạn nêu được ít nhất năm khác biệt giữa lab và production.
- Bạn giải thích tác động phối hợp của replication factor, ISR và `acks=all`.

---

## 6. Bài kiểm tra tổng hợp cuối khóa

### Nhóm A - Happy path

Input:

- 100 order hợp lệ.

Expected:

```text
orders.raw.v1 count       = 100
orders.validated.v1 count = 100
orders.dlq.v1 count       = 0
PostgreSQL orders         = 100
sum(amount)               = sum(revenue)
```

### Nhóm B - Data quality

Input:

- 10 JSON hỏng.
- 10 event thiếu `order_id`.
- 10 event có `amount < 0`.
- 10 event version không hỗ trợ.

Expected:

- Validator không crash loop.
- 40 record đi DLQ.
- DLQ giữ topic, partition, offset, key và error.
- Validated/database count không tăng do record lỗi.

### Nhóm C - Duplicate

Input:

- Cùng event và `event_id` gửi năm lần.
- Cùng payload nhưng năm `event_id` khác nhau.

Expected:

- Trường hợp một: event-level idempotency làm metric tăng một lần.
- Trường hợp hai: `order_id` unique tiếp tục bảo vệ business-level idempotency,
  nên metric vẫn chỉ tăng một lần.

### Nhóm D - Restart

1. Dừng sink sau DB commit nhưng trước Kafka commit.
2. Khởi động lại.

Expected:

- Record được giao lại.
- `processed_events` chặn side effect trùng.
- Offset cuối cùng vẫn tiến lên.

### Nhóm E - Scale

- Chạy 1, 2, 3, 4 consumer cùng group.
- Gửi 1000 event với nhiều key.
- Gửi 1000 event với một hot key.

Expected:

- Tối đa ba consumer có partition.
- Dữ liệu đều scale tốt hơn hot key.
- Ordering của một key vẫn nằm trong một partition.

### Nhóm F - Recovery

- Rebuild bảng mới từ earliest bằng group mới.
- Replay một DLQ record sau khi sửa payload.
- Replay cùng record hai lần.

Expected:

- Bảng rebuild khớp.
- Replay không tạo duplicate business effect.
- Có audit/log cho thao tác replay.

---

## 7. Checklist kiến thức

Chỉ đánh dấu `[x]` khi bạn vừa giải thích được vừa chứng minh bằng lab.

### Kafka cơ bản

- [ ] Tôi phân biệt broker, controller, cluster, topic và partition.
- [ ] Tôi biết record có key, value, headers, timestamp, partition và offset.
- [ ] Tôi biết offset chỉ có ý nghĩa trong một partition.
- [ ] Tôi biết ordering chỉ được đảm bảo trong partition.
- [ ] Tôi giải thích được vì sao dùng `order_id` làm key.

### Producer

- [ ] Tôi hiểu serialization.
- [ ] Tôi hiểu `produce()` bất đồng bộ.
- [ ] Tôi xử lý delivery callback và `flush()`.
- [ ] Tôi phân biệt `acks=0`, `acks=1`, `acks=all`.
- [ ] Tôi hiểu phạm vi của idempotent producer.

### Consumer

- [ ] Tôi hiểu poll loop.
- [ ] Tôi hiểu group và partition assignment.
- [ ] Tôi quan sát được rebalance.
- [ ] Tôi phân biệt current position và committed offset.
- [ ] Tôi biết `auto.offset.reset` chỉ dùng khi không có offset hợp lệ.
- [ ] Tôi đọc được consumer lag.

### Reliability

- [ ] Tôi phân biệt at-most-once, at-least-once và exactly-once.
- [ ] Tôi tái hiện được duplicate do crash.
- [ ] Consumer sink của tôi idempotent.
- [ ] Tôi phân biệt transient, permanent và business error.
- [ ] Tôi có retry hữu hạn và DLQ.
- [ ] Tôi replay DLQ an toàn.

### Storage và consistency

- [ ] Tôi hiểu dual-write problem.
- [ ] Tôi giải thích được Transactional Outbox.
- [ ] Tôi hiểu Outbox vẫn có thể publish duplicate.
- [ ] Tôi phân biệt retention và compaction.
- [ ] Tôi hiểu tombstone.
- [ ] Tôi rebuild được projection từ Kafka log.

### Operations

- [ ] Tôi theo dõi lag, error, retry và DLQ.
- [ ] Tôi có correlation bằng `event_id` và `order_id`.
- [ ] Tôi phân biệt liveness, readiness và business health.
- [ ] Tôi có runbook xử lý lag.
- [ ] Tôi biết local one-broker cluster chưa phải production.

---

## 8. Câu hỏi tự kiểm tra

1. Kafka khác queue truyền thống ở retention và replay như thế nào?
2. Offset 10 của partition 0 có liên quan offset 10 của partition 1 không?
3. Hai consumer cùng group có cùng xử lý một partition tại một thời điểm không?
4. Hai consumer khác group có đọc cùng record không?
5. Topic ba partition có tận dụng mười consumer cùng group không?
6. Vì sao cùng key thường vào cùng partition?
7. Kafka có đảm bảo ordering toàn topic không?
8. Khi nào `auto.offset.reset=earliest` có tác dụng?
9. Commit trước xử lý gây rủi ro gì?
10. Commit sau xử lý gây rủi ro gì?
11. Idempotent producer và idempotent consumer khác nhau thế nào?
12. Vì sao `acks=all` với replication factor 1 chưa tạo high availability?
13. Business rejection có nên retry không?
14. Poison message có thể làm gì với một partition?
15. DLQ cần giữ metadata nào?
16. Vì sao replay cần idempotency?
17. Retention khác compaction như thế nào?
18. Tombstone là gì?
19. Kafka transaction có commit PostgreSQL transaction không?
20. Outbox giải quyết mất event nhưng vì sao vẫn có duplicate?
21. Lag bằng 0 có chứng minh dữ liệu đúng không?
22. Vì sao hot key không được giải quyết chỉ bằng cách tăng consumer?
23. Event time khác processing time như thế nào?
24. Khi nào Kafka Connect phù hợp hơn consumer Python tự viết?
25. Metric nào cần xem đầu tiên khi pipeline chậm?

Nếu chưa trả lời chắc chắn, quay lại stage tương ứng và tạo một thí nghiệm nhỏ.

---

## 9. Thứ tự học nâng cao sau khi hoàn thành

Chỉ chọn một hướng tại một thời điểm:

1. **Schema governance:** Avro hoặc Protobuf, Schema Registry, compatibility.
2. **Kafka Connect/CDC:** Debezium PostgreSQL, source/sink connector.
3. **Stream processing:** Kafka Streams, Flink hoặc Spark Structured Streaming.
4. **Observability:** Prometheus, Grafana, OpenTelemetry.
5. **Production cluster:** ba broker, controller quorum, TLS/SASL/ACL.
6. **Data lake:** sink event vào object storage, partitioning và table format.
7. **Testing:** contract test, integration test bằng Docker và failure injection.

Không cần học tất cả cùng lúc. Với Data Engineer mới nhập môn, khả năng dự đoán
record đi đâu, offset thay đổi thế nào và điều gì xảy ra khi process chết quan
trọng hơn việc biết nhiều framework.
