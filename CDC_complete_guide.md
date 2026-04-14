# CDC (Change Data Capture) 완전 가이드

> **CDC**는 데이터베이스에서 발생하는 INSERT · UPDATE · DELETE 변경 사항을 실시간으로 감지하고 다른 시스템에 전파하는 기술이다. ETL의 "배치 복사"와 달리 **변경된 행만 골라서 즉시 전달**한다.

---

## 목차

1. [CDC 개요 및 필요성](#1-cdc-개요-및-필요성)
2. [CDC 4가지 방식](#2-cdc-4가지-방식)
   - 2.1 로그 기반 (Log-based)
   - 2.2 트리거 기반 (Trigger-based)
   - 2.3 쿼리 기반 (Query-based)
   - 2.4 타임스탬프 기반 (Timestamp-based)
3. [전체 기술 스택 흐름](#3-전체-기술-스택-흐름)
4. [Log-based CDC 상세](#4-log-based-cdc-상세)
5. [주요 CDC 소프트웨어](#5-주요-cdc-소프트웨어)
   - 5.1 Debezium
   - 5.2 Maxwell's Daemon
   - 5.3 Canal (Alibaba)
   - 5.4 AWS DMS
   - 5.5 Striim
6. [CDC 이벤트 구조](#6-cdc-이벤트-구조)
7. [Python CDC 구현 예제](#7-python-cdc-구현-예제)
8. [방식 비교 요약](#8-방식-비교-요약)

---

## 1. CDC 개요 및 필요성

### 왜 CDC인가?

전통적인 배치 ETL은 전체 테이블을 주기적으로 복사하기 때문에:
- 대용량 테이블에서 불필요한 I/O 발생
- 마지막 배치 이후 변경분만 필요함에도 전체를 처리
- 실시간 동기화가 불가능 (수 분~수 시간 지연)

CDC는 이 문제를 해결한다.

```
전통 ETL:
  [Source DB 전체 테이블] ──────────────▶ [Target]
  매 시간 10GB 복사 (변경은 10MB뿐)

CDC:
  [Source DB] ──(변경된 10MB만)──────▶ [Target]
  초 단위 실시간 전파
```

### CDC 주요 활용 사례

- 마이크로서비스 간 데이터 동기화 (이벤트 드리븐 아키텍처)
- OLTP → OLAP 실시간 복제 (Data Warehouse 동기화)
- 캐시(Redis) 무효화 및 갱신
- 검색 엔진(Elasticsearch) 인덱스 실시간 업데이트
- 감사 로그(Audit Log) 생성
- 재해 복구(DR) 및 읽기 복제본 동기화

---

## 2. CDC 4가지 방식

### 2.1 로그 기반 (Log-based CDC)

**핵심 원리**: DB 내부 트랜잭션 로그(WAL, Binlog)를 직접 읽어 변경 이벤트를 추출한다.

```
PostgreSQL WAL:
  BEGIN
  table public.orders: INSERT: id[integer]:1 customer_id[integer]:42 amount[numeric]:9900
  COMMIT

MySQL Binlog:
  # at 1234
  WRITE_ROWS_EVENT table_id=56 (orders)
    INSERT INTO orders VALUES (1, 42, 9900, '2024-01-15')
```

**장점:**
- 소스 DB에 추가 부하 없음 (로그는 DB가 이미 생성)
- 밀리초 단위 실시간 처리
- INSERT / UPDATE / DELETE 모두 감지
- 변경 전(before) · 후(after) 값 모두 캡처 가능
- DDL 변경(ALTER TABLE)도 감지 가능

**단점:**
- DB마다 로그 포맷이 다름 (설정 필요)
- DBA 권한 필요 (replication slot, binlog 접근)
- PostgreSQL은 `wal_level=logical` 설정 필요

**사전 설정 (PostgreSQL):**
```sql
-- postgresql.conf 설정
ALTER SYSTEM SET wal_level = logical;
ALTER SYSTEM SET max_replication_slots = 10;
ALTER SYSTEM SET max_wal_senders = 10;
SELECT pg_reload_conf();

-- Replication slot 생성
SELECT pg_create_logical_replication_slot('debezium_slot', 'pgoutput');

-- 테이블에 REPLICA IDENTITY 설정 (UPDATE/DELETE 시 before 값 포함)
ALTER TABLE orders REPLICA IDENTITY FULL;
```

**사전 설정 (MySQL):**
```sql
-- my.cnf
[mysqld]
server-id         = 1
log_bin           = mysql-bin
binlog_format     = ROW          -- ROW 형식 필수
binlog_row_image  = FULL         -- before/after 모두 캡처
expire_logs_days  = 10

-- Debezium용 사용자 권한
CREATE USER 'debezium'@'%' IDENTIFIED BY 'password';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'debezium'@'%';
```

**대표 도구:** Debezium, Maxwell's Daemon, Canal, Striim, AWS DMS

---

### 2.2 트리거 기반 (Trigger-based CDC)

**핵심 원리**: DB 트리거를 이용해 DML 발생 시 변경 내역을 별도 감사 테이블에 기록한다.

```sql
-- 감사 테이블 생성
CREATE TABLE orders_cdc_log (
    log_id      BIGSERIAL PRIMARY KEY,
    operation   VARCHAR(6),          -- INSERT / UPDATE / DELETE
    changed_at  TIMESTAMP DEFAULT NOW(),
    old_data    JSONB,
    new_data    JSONB
);

-- INSERT 트리거
CREATE OR REPLACE FUNCTION fn_orders_insert_cdc()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO orders_cdc_log(operation, new_data)
    VALUES ('INSERT', row_to_json(NEW));
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_orders_insert
AFTER INSERT ON orders
FOR EACH ROW EXECUTE FUNCTION fn_orders_insert_cdc();

-- UPDATE 트리거 (before/after 모두 기록)
CREATE OR REPLACE FUNCTION fn_orders_update_cdc()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO orders_cdc_log(operation, old_data, new_data)
    VALUES ('UPDATE', row_to_json(OLD), row_to_json(NEW));
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_orders_update
AFTER UPDATE ON orders
FOR EACH ROW EXECUTE FUNCTION fn_orders_update_cdc();

-- DELETE 트리거
CREATE OR REPLACE FUNCTION fn_orders_delete_cdc()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO orders_cdc_log(operation, old_data)
    VALUES ('DELETE', row_to_json(OLD));
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_orders_delete
AFTER DELETE ON orders
FOR EACH ROW EXECUTE FUNCTION fn_orders_delete_cdc();
```

**감사 테이블 폴링 (Python):**
```python
import psycopg2
import json
from datetime import datetime

def poll_trigger_cdc(conn_str: str, last_log_id: int = 0):
    """감사 테이블에서 새 CDC 이벤트를 폴링"""
    conn = psycopg2.connect(conn_str)
    cur  = conn.cursor()

    cur.execute("""
        SELECT log_id, operation, changed_at, old_data, new_data
        FROM orders_cdc_log
        WHERE log_id > %s
        ORDER BY log_id ASC
        LIMIT 1000
    """, (last_log_id,))

    events = []
    for row in cur.fetchall():
        log_id, op, ts, old, new = row
        events.append({
            "log_id":    log_id,
            "op":        op,
            "timestamp": ts.isoformat(),
            "before":    old,
            "after":     new,
        })
        last_log_id = log_id

    cur.close()
    conn.close()
    return events, last_log_id
```

**장점:** 구현 단순, DB 기능만으로 완결, 별도 에이전트 불필요
**단점:** 모든 DML마다 감사 테이블에 추가 쓰기 → 성능 2배 부하, 대용량 비적합

---

### 2.3 쿼리 기반 (Query-based CDC)

**핵심 원리**: 주기적으로 SELECT를 실행하여 이전 스냅샷과 비교하거나, 특정 컬럼(PK, checksum)으로 변경 여부를 탐지한다.

```python
import pandas as pd
import psycopg2
import hashlib
import json
from datetime import datetime, timedelta


def query_based_cdc(conn_str: str, table: str, pk_col: str,
                    last_snapshot: dict) -> tuple[list, dict]:
    """
    PK 기준 전체 스냅샷 비교 방식
    last_snapshot: {pk_value: row_hash}
    """
    conn = psycopg2.connect(conn_str)
    df   = pd.read_sql(f"SELECT * FROM {table}", conn)
    conn.close()

    events       = []
    new_snapshot = {}

    for _, row in df.iterrows():
        pk  = row[pk_col]
        row_hash = hashlib.md5(
            json.dumps(row.to_dict(), default=str, sort_keys=True).encode()
        ).hexdigest()

        new_snapshot[pk] = row_hash

        if pk not in last_snapshot:
            # 신규 행: INSERT
            events.append({"op": "INSERT", "after": row.to_dict()})
        elif last_snapshot[pk] != row_hash:
            # 해시 변경: UPDATE
            events.append({"op": "UPDATE", "after": row.to_dict()})

    # 삭제된 행 탐지 (이전 스냅샷에는 있었으나 현재 없는 PK)
    for pk in last_snapshot:
        if pk not in new_snapshot:
            events.append({"op": "DELETE", "pk": pk})

    return events, new_snapshot


def timestamp_based_cdc(conn_str: str, table: str,
                        ts_col: str, since: datetime) -> list:
    """
    updated_at 컬럼 기반 증분 조회
    삭제 이벤트는 감지 불가
    """
    conn = psycopg2.connect(conn_str)
    df   = pd.read_sql(f"""
        SELECT * FROM {table}
        WHERE {ts_col} > %s
        ORDER BY {ts_col} ASC
    """, conn, params=(since,))
    conn.close()

    events = []
    for _, row in df.iterrows():
        events.append({"op": "UPSERT", "after": row.to_dict()})
    return events


# 실행 예시
if __name__ == "__main__":
    import time

    snapshot = {}
    while True:
        events, snapshot = query_based_cdc(
            conn_str  = "postgresql://user:pw@host/db",
            table     = "orders",
            pk_col    = "id",
            last_snapshot = snapshot
        )
        for e in events:
            print(f"[{e['op']}] {e}")
        time.sleep(60)  # 60초 폴링
```

**장점:** 구현 단순, DB 종류 무관, 별도 권한 불필요
**단점:** 대용량 테이블에서 비효율, 삭제 감지 불완전, 지연 발생

---

### 2.4 타임스탬프 기반 (Timestamp-based CDC)

쿼리 기반의 한 형태로, `updated_at` 같은 타임스탬프 컬럼으로 변경 행만 조회한다. 구현이 가장 단순하지만 삭제 감지가 불가능하고, 타임스탬프 컬럼이 반드시 존재해야 한다.

```sql
-- 테이블에 타임스탬프 컬럼이 있어야 함
ALTER TABLE orders ADD COLUMN updated_at TIMESTAMP DEFAULT NOW();

-- 자동 갱신 트리거
CREATE OR REPLACE FUNCTION fn_set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_orders_updated_at
BEFORE UPDATE ON orders
FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at();

-- CDC 쿼리: 마지막 실행 시각 이후 변경된 행만 가져오기
SELECT * FROM orders
WHERE updated_at > :last_run_at
ORDER BY updated_at ASC;
```

---

## 3. 전체 기술 스택 흐름

```
┌──────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────┐
│  소스 DB  │    │  CDC 커넥터   │    │   메시지 큐   │    │  스트림 처리  │    │ 싱크 시스템 │
│──────────│    │──────────────│    │──────────────│    │──────────────│    │──────────│
│ MySQL    │    │ Debezium     │    │ Apache Kafka │    │ Apache Flink │    │Snowflake │
│ Postgres │───▶│ Maxwell      │───▶│ AWS Kinesis  │───▶│ Kafka Streams│───▶│ BigQuery │
│ Oracle   │    │ Canal        │    │ Google Pub/Sub│   │ Spark Struct.│    │ Redis    │
│ MongoDB  │    │ AWS DMS      │    │ Azure EH     │    │ AWS Glue     │    │Elastic.  │
└──────────┘    └──────────────┘    └──────────────┘    └──────────────┘    └──────────┘
  WAL/Binlog       Log Reader          Topic/Offset        실시간 변환       적재/동기화
```

---

## 4. Log-based CDC 상세

### WAL(Write-Ahead Log) 동작 원리

```
트랜잭션 실행:
  UPDATE orders SET status='SHIPPED' WHERE id=1;

  1. WAL에 변경 내역 먼저 기록 (before: 'PENDING' → after: 'SHIPPED')
  2. 데이터 파일에 실제 반영
  3. 커밋 시 WAL에 COMMIT 레코드 추가

CDC 커넥터 동작:
  1. Replication Slot을 통해 WAL 스트리밍 구독
  2. LSN(Log Sequence Number)으로 현재 읽은 위치 추적
  3. 커밋된 트랜잭션의 이벤트만 추출
  4. Kafka 토픽으로 발행

LSN 추적:
  [LSN: 0/1A3F000] BEGIN
  [LSN: 0/1A3F100] UPDATE orders: before={status:PENDING} after={status:SHIPPED}
  [LSN: 0/1A3F200] COMMIT
              ↑
         커넥터는 이 LSN을 체크포인트로 저장
         재시작 시 여기서부터 재개 → 정확히 한 번(Exactly-once) 보장
```

### Debezium Kafka Connect 설정 (PostgreSQL)

```json
{
  "name": "orders-pg-connector",
  "config": {
    "connector.class":               "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname":             "postgres-host",
    "database.port":                 "5432",
    "database.user":                 "debezium",
    "database.password":             "secret",
    "database.dbname":               "mydb",
    "database.server.name":          "pgserver1",
    "table.include.list":            "public.orders,public.customers",
    "plugin.name":                   "pgoutput",
    "slot.name":                     "debezium_slot",
    "publication.name":              "debezium_pub",
    "snapshot.mode":                 "initial",
    "tombstones.on.delete":          "true",
    "decimal.handling.mode":         "double",
    "time.precision.mode":           "connect",
    "transforms":                    "route",
    "transforms.route.type":         "org.apache.kafka.connect.transforms.ReplaceField$Value",
    "key.converter":                 "io.confluent.kafka.serializers.KafkaAvroSerializer",
    "value.converter":               "io.confluent.kafka.serializers.KafkaAvroSerializer",
    "schema.registry.url":           "http://schema-registry:8081"
  }
}
```

**생성되는 Kafka 토픽 패턴:** `{server.name}.{schema}.{table}`
- `pgserver1.public.orders`
- `pgserver1.public.customers`

---

## 5. 주요 CDC 소프트웨어

### 5.1 Debezium

Red Hat이 오픈소스로 개발한 CDC 플랫폼. Kafka Connect 위에서 동작한다.

**지원 DB:** MySQL, PostgreSQL, Oracle, SQL Server, MongoDB, DB2, Cassandra

**특징:**
- Kafka Connect 기반으로 분산·확장이 용이
- Schema Registry와 연동해 스키마 진화(Schema Evolution) 지원
- 정확히 한 번(Exactly-once) 시맨틱 보장
- SMT(Single Message Transform)로 이벤트 가공 가능

**Docker 빠른 시작:**
```bash
docker run -d --name kafka-connect \
  -p 8083:8083 \
  -e BOOTSTRAP_SERVERS=kafka:9092 \
  -e GROUP_ID=1 \
  -e CONFIG_STORAGE_TOPIC=connect-configs \
  -e OFFSET_STORAGE_TOPIC=connect-offsets \
  -e STATUS_STORAGE_TOPIC=connect-status \
  debezium/connect:2.4
```

---

### 5.2 Maxwell's Daemon

MySQL Binlog를 읽어 JSON으로 Kafka, Kinesis, SQS 등에 발행하는 경량 CDC 도구.

**특징:** 설정이 단순, MySQL 전용, Java 기반 단일 프로세스

```bash
# 실행
bin/maxwell \
  --user=maxwell --password=secret --host=mysql-host \
  --producer=kafka --kafka.bootstrap.servers=kafka:9092 \
  --kafka_topic=maxwell_events \
  --filter='include: mydb.orders, mydb.customers'
```

**출력 이벤트 예시:**
```json
{
  "database": "mydb",
  "table":    "orders",
  "type":     "update",
  "ts":       1705123456,
  "xid":      12345,
  "data": {
    "id":     1,
    "status": "SHIPPED",
    "amount": 9900
  },
  "old": {
    "status": "PENDING"
  }
}
```

---

### 5.3 Canal (Alibaba)

Alibaba가 오픈소스로 공개한 MySQL Binlog 파서. 내부적으로 MySQL Slave 프로토콜을 구현한다.

**특징:** 고성능, 중국 대형 서비스에서 검증, Java 기반, RocketMQ/Kafka 연동

```java
// Canal Java 클라이언트 예시
CanalConnector connector = CanalConnectors.newSingleConnector(
    new InetSocketAddress("canal-server", 11111), "example", "", "");

connector.connect();
connector.subscribe(".*\\..*");  // 모든 DB·테이블 구독

while (true) {
    Message message = connector.getWithoutAck(100);  // 100건씩 배치
    long batchId = message.getId();

    for (CanalEntry.Entry entry : message.getEntries()) {
        if (entry.getEntryType() == EntryType.ROWDATA) {
            RowChange rowChange = RowChange.parseFrom(entry.getStoreValue());
            for (RowData rowData : rowChange.getRowDatasList()) {
                System.out.println("EventType: " + rowChange.getEventType());
                System.out.println("After: " + rowData.getAfterColumnsList());
            }
        }
    }
    connector.ack(batchId);
}
```

---

### 5.4 AWS DMS (Database Migration Service)

AWS 관리형 CDC 서비스. 초기 로드(Full Load) + 지속적 복제(CDC) 통합 지원.

**특징:** 관리형(설정만 하면 됨), 타 AWS 서비스와 연동 용이, 비용 발생

**지원 조합:** RDS → S3, Kinesis, Kafka, Redshift, DynamoDB 등

**설정 예시 (Python SDK):**
```python
import boto3

dms = boto3.client('dms', region_name='ap-northeast-2')

# 복제 태스크 생성 (CDC 모드)
response = dms.create_replication_task(
    ReplicationTaskIdentifier = 'orders-cdc-task',
    SourceEndpointArn         = 'arn:aws:dms:...:endpoint:source-rds',
    TargetEndpointArn         = 'arn:aws:dms:...:endpoint:target-kinesis',
    ReplicationInstanceArn    = 'arn:aws:dms:...:rep:...',
    MigrationType             = 'cdc',           # full-load | cdc | full-load-and-cdc
    TableMappings             = json.dumps({
        "rules": [{
            "rule-type":   "selection",
            "rule-id":     "1",
            "rule-name":   "include-orders",
            "object-locator": {
                "schema-name": "public",
                "table-name":  "orders"
            },
            "rule-action": "include"
        }]
    }),
    ReplicationTaskSettings = json.dumps({
        "TargetMetadata": {
            "TargetSchema":    "",
            "SupportLobs":     True,
            "FullLobMode":     False,
        },
        "Logging": {
            "EnableLogging": True
        }
    })
)
print("Task ARN:", response['ReplicationTask']['ReplicationTaskArn'])
```

---

### 5.5 Striim

실시간 데이터 통합 플랫폼. 엔터프라이즈 환경에서 CDC + 스트림 처리 + 적재를 단일 플랫폼으로 제공한다.

**특징:** GUI 기반 파이프라인 설계, SQL-like 쿼리로 변환 정의, Oracle · SAP HANA 지원

---

## 6. CDC 이벤트 구조

Debezium이 생성하는 Kafka 메시지 구조:

```json
{
  "schema": { "...": "Avro 스키마 정보" },
  "payload": {
    "before": {
      "id":     1,
      "status": "PENDING",
      "amount": 9900
    },
    "after": {
      "id":     1,
      "status": "SHIPPED",
      "amount": 9900
    },
    "source": {
      "version":   "2.4.0.Final",
      "connector": "postgresql",
      "name":      "pgserver1",
      "ts_ms":     1705123456789,
      "db":        "mydb",
      "schema":    "public",
      "table":     "orders",
      "lsn":       12345678,
      "xmin":      null
    },
    "op":     "u",       -- c=insert, u=update, d=delete, r=read(snapshot)
    "ts_ms":  1705123456789
  }
}
```

### 이벤트 op 코드

| op | 의미 | before | after |
|----|------|--------|-------|
| `c` | INSERT (create) | null | 신규 행 |
| `u` | UPDATE | 변경 전 | 변경 후 |
| `d` | DELETE | 삭제된 행 | null |
| `r` | READ (snapshot) | null | 스냅샷 행 |

---

## 7. Python CDC 구현 예제

### Kafka Consumer로 CDC 이벤트 처리

```python
from confluent_kafka import Consumer, KafkaError
import json
import logging
from typing import Callable

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


class CDCEventProcessor:
    """Debezium CDC 이벤트 소비 및 처리"""

    def __init__(self, bootstrap_servers: str, group_id: str, topics: list[str]):
        self.consumer = Consumer({
            "bootstrap.servers":  bootstrap_servers,
            "group.id":           group_id,
            "auto.offset.reset":  "earliest",
            "enable.auto.commit": False,
        })
        self.consumer.subscribe(topics)
        self.handlers: dict[str, list[Callable]] = {
            "c": [], "u": [], "d": [], "r": []
        }

    def on(self, op: str):
        """이벤트 핸들러 등록 데코레이터"""
        def decorator(fn: Callable):
            self.handlers.get(op, []).append(fn)
            return fn
        return decorator

    def parse_event(self, raw_value: bytes) -> dict | None:
        """Debezium JSON 이벤트 파싱"""
        try:
            msg = json.loads(raw_value)
            payload = msg.get("payload", msg)
            return {
                "op":     payload.get("op"),
                "before": payload.get("before"),
                "after":  payload.get("after"),
                "table":  payload.get("source", {}).get("table"),
                "ts_ms":  payload.get("ts_ms"),
            }
        except Exception as e:
            log.error(f"이벤트 파싱 실패: {e}")
            return None

    def run(self):
        log.info("CDC 이벤트 처리 시작")
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    log.error(f"Kafka 오류: {msg.error()}")
                    break

                event = self.parse_event(msg.value())
                if event and event["op"]:
                    for handler in self.handlers.get(event["op"], []):
                        handler(event)

                self.consumer.commit(msg)

        finally:
            self.consumer.close()


# ── 사용 예시

import redis
import psycopg2

processor = CDCEventProcessor(
    bootstrap_servers = "kafka:9092",
    group_id          = "cdc-consumer-group",
    topics            = ["pgserver1.public.orders"]
)

r = redis.Redis(host="redis", port=6379, decode_responses=True)


@processor.on("c")
def on_insert(event: dict):
    """신규 주문 → 캐시에 저장"""
    row = event["after"]
    cache_key = f"order:{row['id']}"
    r.hset(cache_key, mapping=row)
    r.expire(cache_key, 3600)
    log.info(f"[INSERT] 주문 {row['id']} 캐시 저장")


@processor.on("u")
def on_update(event: dict):
    """주문 상태 변경 → 캐시 갱신"""
    before = event["before"]
    after  = event["after"]
    cache_key = f"order:{after['id']}"

    if before and before.get("status") != after.get("status"):
        r.hset(cache_key, "status", after["status"])
        log.info(f"[UPDATE] 주문 {after['id']} 상태: {before.get('status')} → {after['status']}")


@processor.on("d")
def on_delete(event: dict):
    """주문 삭제 → 캐시 제거"""
    row = event["before"]
    if row:
        cache_key = f"order:{row['id']}"
        r.delete(cache_key)
        log.info(f"[DELETE] 주문 {row['id']} 캐시 삭제")


if __name__ == "__main__":
    processor.run()
```

### PostgreSQL Logical Replication 직접 구현 (psycopg2)

```python
import psycopg2
import psycopg2.extras
import json
import select


def listen_pg_logical_replication(dsn: str, slot_name: str):
    """
    PostgreSQL Logical Replication을 직접 구현
    Debezium 없이 Python만으로 CDC 구현
    """
    conn = psycopg2.connect(
        dsn,
        connection_factory=psycopg2.extras.LogicalReplicationConnection
    )
    cur = conn.cursor()

    # Replication Slot이 없으면 생성
    try:
        cur.create_replication_slot(slot_name, output_plugin="wal2json")
        print(f"Replication slot '{slot_name}' 생성 완료")
    except psycopg2.errors.DuplicateObject:
        print(f"Replication slot '{slot_name}' 이미 존재, 재사용")

    # 스트리밍 시작
    cur.start_replication(
        slot_name   = slot_name,
        decode      = True,
        options     = {
            "pretty-print":           1,
            "include-timestamp":      1,
            "include-lsn":            1,
            "include-transaction":    1,
        }
    )

    def process_message(msg):
        """WAL 메시지 처리 콜백"""
        data = json.loads(msg.payload)

        for change in data.get("change", []):
            event = {
                "lsn":       str(msg.data_start),
                "timestamp": data.get("timestamp"),
                "op":        change.get("kind"),
                "schema":    change.get("schema"),
                "table":     change.get("table"),
                "columns":   change.get("columnnames", []),
                "values":    change.get("columnvalues", []),
                "old_keys":  change.get("oldkeys", {}),
            }
            print(json.dumps(event, ensure_ascii=False, default=str))

        msg.cursor.send_feedback(flush_lsn=msg.data_start)  # 처리 완료 확인

    print("CDC 스트리밍 시작...")
    cur.consume_stream(process_message)
```

---

## 8. 방식 비교 요약

| 항목 | Log-based | Trigger-based | Query-based | Timestamp-based |
|------|-----------|---------------|-------------|-----------------|
| **실시간성** | 밀리초 | 밀리초~초 | 분 단위 | 분 단위 |
| **소스 부하** | 없음 | 높음 (쓰기 2배) | 중간 (SELECT) | 낮음 |
| **삭제 감지** | 가능 | 가능 | 전체 비교 필요 | 불가 |
| **구현 복잡도** | 높음 | 낮음 | 낮음 | 매우 낮음 |
| **DB 권한** | Replication 권한 | 트리거 생성 | SELECT | SELECT |
| **DDL 변경 감지** | 가능 | 불가 | 불가 | 불가 |
| **Before 값 캡처** | 가능 | 가능 | 불가 | 불가 |
| **대용량 적합** | 최적 | 부적합 | 보통 | 보통 |
| **주요 도구** | Debezium, Maxwell | 직접 구현, Tungsten | Sqoop, Airbyte | Airbyte, 직접 구현 |
| **권장 환경** | 프로덕션 실시간 | 소규모 레거시 | 배치 마이그레이션 | 단순 동기화 |

### 선택 가이드

```
Q1. 실시간 동기화가 필요한가?
  → 예: Log-based CDC (Debezium)
  → 아니오: 다음으로

Q2. DB 부하를 최소화해야 하는가?
  → 예: Timestamp-based (단, 삭제 감지 포기)
  → 아니오: 다음으로

Q3. 삭제 이벤트를 감지해야 하는가?
  → 예: Trigger-based 또는 Query-based (전체 비교)
  → 아니오: Timestamp-based

Q4. 기존 DB에 트리거 추가가 가능한가?
  → 예: Trigger-based (소규모) 또는 Log-based (대규모)
  → 아니오: Query-based
```

---

## 참고 자료

- Debezium 공식 문서: https://debezium.io/documentation/
- Maxwell's Daemon: https://maxwells-daemon.io/
- Canal GitHub: https://github.com/alibaba/canal
- AWS DMS 문서: https://docs.aws.amazon.com/dms/
- Kleppmann, M. (2017). *Designing Data-Intensive Applications*. O'Reilly.
