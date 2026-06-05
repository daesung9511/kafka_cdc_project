# Kafka Data Engineering Projects

This repository contains two Kafka-based data engineering projects.

- **Project 1:** CSV ETL pipeline with Kafka and PostgreSQL
- **Project 2:** PostgreSQL CDC pipeline with triggers, `LISTEN/NOTIFY`, Kafka, DLQ, and PostgreSQL replication

The purpose of these projects is to practice Kafka producer/consumer design, PostgreSQL loading, CDC modeling, offset management, idempotent database writes, and basic reliability patterns.

---

## Repository Structure

```text
kafkaProject/
├── app/
│   ├── config.py                  # Shared Kafka/Postgres configuration
│   ├── connector.py               # SQLAlchemy Postgres connector
│   ├── producer.py                # Project 2 CDC producer
│   └── consumer.py                # Project 2 CDC consumer
├── docker/
│   ├── source-init/
│   │   └── 01_source_schema.sql   # Source DB schema, trigger, CDC table
│   └── dest-init/
│       └── 01_dest_schema.sql     # Destination DB schema
├── proj1/
│   ├── Employee_Salaries.csv      # CSV input file for Project 1
│   └── kafka_project1.py          # Project 1 producer/consumer app
├── scripts/
│   ├── create_topics.py           # Kafka topic creation helper
│   ├── insert_samples.sql         # Project 2 sample inserts
│   └── change_samples.sql         # Project 2 sample updates/deletes/inserts
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## Prerequisites

Install Docker Desktop and Python 3.10+.

Create and activate a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Start Kafka, ZooKeeper, Kafka UI, source PostgreSQL, and destination PostgreSQL:

```bash
docker compose up -d
```

Check running containers:

```bash
docker ps
```

Kafka UI:

```text
http://localhost:8085
```

Important local ports:

| Service | Host Port | Container Port |
|---|---:|---:|
| Kafka external listener | 29092 | 29092 |
| Kafka internal listener | 9092 | 9092 |
| Source PostgreSQL | 5433 | 5432 |
| Destination PostgreSQL | 5434 | 5432 |
| Kafka UI | 8085 | 8080 |

When Python runs from the laptop, use:

```text
localhost:29092
```

When Python runs inside another Docker container, use:

```text
kafka:9092
```

---

# Project 1: CSV ETL Pipeline

## Objective

Project 1 builds a Kafka ETL pipeline from CSV to PostgreSQL.

```text
Employee_Salaries.csv
        |
        | Producer: extract + transform
        v
Kafka topic: employee_salaries
        |
        | Consumer: load
        v
PostgreSQL tables
```

The producer reads `Employee_Salaries.csv`, applies filtering and transformation rules, and sends valid employee records to Kafka. The consumer reads Kafka messages, inserts each employee into PostgreSQL, and continuously updates total salary by department.

## Transformation Rules

The producer applies the following transformations:

1. Keep only these departments:
   - `ECC`
   - `CIT`
   - `EMS`
2. Include employees hired after `2010-01-01`.
3. Round salary down to an integer.
4. Send transformed records to Kafka.

The date rule is implemented as:

```python
if hire_date <= date(2010, 1, 1):
    return None
```

This keeps employees hired after January 1, 2010 and matches the project success criteria.

## Project 1 Tables

`department_employee` stores every valid employee row:

```sql
CREATE TABLE IF NOT EXISTS department_employee (
    department VARCHAR(200),
    department_division VARCHAR(200),
    position_title VARCHAR(200),
    hire_date DATE,
    salary INT
);
```

`department_employee_salary` stores the running total salary by department:

```sql
CREATE TABLE IF NOT EXISTS department_employee_salary (
    department VARCHAR(200) PRIMARY KEY,
    total_salary BIGINT
);
```

The consumer updates department totals with PostgreSQL upsert:

```sql
INSERT INTO department_employee_salary (
    department,
    total_salary
)
VALUES (
    :department,
    :salary
)
ON CONFLICT (department)
DO UPDATE SET
    total_salary = department_employee_salary.total_salary + EXCLUDED.total_salary;
```

This means:

- If the department does not exist yet, insert it.
- If the department already exists, add the current employee salary to the existing total.

## Run Project 1

Use the source PostgreSQL database for Project 1:

```bash
export DATABASE_URL="postgresql+psycopg2://postgres:postgres@localhost:5433/source_db"
export KAFKA_BOOTSTRAP_SERVERS="localhost:29092"
export KAFKA_TOPIC="employee_salaries"
```

Initialize Project 1 tables:

```bash
python proj1/kafka_project1.py init-db --reset
```

Start the consumer in Terminal 1:

```bash
python proj1/kafka_project1.py consume
```

Run the producer in Terminal 2:

```bash
python proj1/kafka_project1.py produce --csv proj1/Employee_Salaries.csv
```

Check expected totals from the CSV transformation:

```bash
python proj1/kafka_project1.py expected --csv proj1/Employee_Salaries.csv
```

Expected output:

```text
CIT: 9,102,142
ECC: 2,042,698
EMS: 3,779,570
```

Check totals loaded into PostgreSQL:

```bash
python proj1/kafka_project1.py db-totals
```

Or query manually:

```bash
docker exec -it cdc_source_db psql -U postgres -d source_db
```

```sql
SELECT *
FROM department_employee_salary
ORDER BY department;
```

## Project 1 Notes

Project 1 is a batch-style pipeline. If the producer is run multiple times without resetting the tables, the consumer may add the same salaries again. For a clean test, reset the Project 1 tables:

```bash
python proj1/kafka_project1.py init-db --reset
```

If Kafka offsets make the consumer skip older messages during testing, use a new topic or a new consumer group.

---

# Project 2: PostgreSQL CDC Pipeline

## Objective

Project 2 builds a CDC-style replication pipeline between two PostgreSQL databases.

```text
source_db.employees
        |
        | PostgreSQL trigger + function
        v
source_db.emp_cdc
        |
        | pg_notify wakes up producer
        v
Python producer
        |
        | sends CDC events
        v
Kafka topic: KafkaCDCProject
        |
        | Python consumer
        v
dest_db.employees
```

Any `INSERT`, `UPDATE`, or `DELETE` on the source `employees` table is captured into the source `emp_cdc` table. The producer reads new CDC rows and publishes them to Kafka. The consumer reads Kafka messages and applies the same changes to the destination `employees` table.

The design uses both:

- **Durable event log:** `emp_cdc`
- **Lightweight event signal:** PostgreSQL `LISTEN/NOTIFY`

`LISTEN/NOTIFY` reduces unnecessary polling, but it is not treated as the source of truth. The producer always reads from `emp_cdc` using `producer_offset.last_cdc_id`.

---

## Project 2 Source DB Schema

The source database has three main tables.

### 1. `employees`

`employees` is the source table whose changes are captured.

```sql
CREATE TABLE IF NOT EXISTS employees (
    emp_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    dob DATE,
    city VARCHAR(100),
    salary INT
);
```

### 2. `emp_cdc`

`emp_cdc` is an append-only event log. It stores every source table change as a CDC event.

```sql
CREATE TABLE IF NOT EXISTS emp_cdc (
    cdc_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    emp_id INT NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    dob DATE,
    city VARCHAR(100),
    salary INT,
    action VARCHAR(20) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT emp_cdc_action_check
    CHECK (action IN ('INSERT', 'UPDATE', 'DELETE'))
);
```

Important design point:

- `cdc_id` is the event ID.
- `emp_id` is the source employee ID.
- `action` tells the consumer whether to insert, update, or delete.
- `created_at` records when the CDC event was created.

### 3. `producer_offset`

`producer_offset` tracks how far the producer has read from `emp_cdc`.

```sql
CREATE TABLE IF NOT EXISTS producer_offset (
    producer_name VARCHAR(100) PRIMARY KEY,
    last_cdc_id BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

`producer_name` is the primary key so the producer can use PostgreSQL upsert:

```sql
INSERT INTO producer_offset (
    producer_name,
    last_cdc_id,
    created_at,
    updated_at
)
VALUES (
    :producer_name,
    :last_cdc_id,
    NOW(),
    NOW()
)
ON CONFLICT (producer_name)
DO UPDATE SET
    last_cdc_id = GREATEST(
        producer_offset.last_cdc_id,
        EXCLUDED.last_cdc_id
    ),
    updated_at = NOW();
```

`GREATEST()` prevents the offset from moving backward accidentally.

---

## Trigger Function and Event Notification

The trigger function writes the changed row into `emp_cdc`. It also sends a PostgreSQL notification using `pg_notify`.

`NEW` and `OLD` are PostgreSQL trigger variables:

| Event | Use | Reason |
|---|---|---|
| `INSERT` | `NEW` | The row did not exist before |
| `UPDATE` | `NEW` | The destination should receive the latest row |
| `DELETE` | `OLD` | The row is gone, so only the old row is available |

```sql
CREATE OR REPLACE FUNCTION trigger_cdc()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_cdc_id BIGINT;
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO emp_cdc (
            emp_id,
            first_name,
            last_name,
            dob,
            city,
            salary,
            action
        )
        VALUES (
            NEW.emp_id,
            NEW.first_name,
            NEW.last_name,
            NEW.dob,
            NEW.city,
            NEW.salary,
            'INSERT'
        )
        RETURNING cdc_id INTO v_cdc_id;

        PERFORM pg_notify('emp_cdc_events', v_cdc_id::TEXT);

        RETURN NEW;

    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO emp_cdc (
            emp_id,
            first_name,
            last_name,
            dob,
            city,
            salary,
            action
        )
        VALUES (
            NEW.emp_id,
            NEW.first_name,
            NEW.last_name,
            NEW.dob,
            NEW.city,
            NEW.salary,
            'UPDATE'
        )
        RETURNING cdc_id INTO v_cdc_id;

        PERFORM pg_notify('emp_cdc_events', v_cdc_id::TEXT);

        RETURN NEW;

    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO emp_cdc (
            emp_id,
            first_name,
            last_name,
            dob,
            city,
            salary,
            action
        )
        VALUES (
            OLD.emp_id,
            OLD.first_name,
            OLD.last_name,
            OLD.dob,
            OLD.city,
            OLD.salary,
            'DELETE'
        )
        RETURNING cdc_id INTO v_cdc_id;

        PERFORM pg_notify('emp_cdc_events', v_cdc_id::TEXT);

        RETURN OLD;
    END IF;

    RETURN NULL;
END;
$$;
```

Attach the trigger to the source table:

```sql
DROP TRIGGER IF EXISTS trg_emp_cdc ON employees;

CREATE TRIGGER trg_emp_cdc
AFTER INSERT OR UPDATE OR DELETE
ON employees
FOR EACH ROW
EXECUTE FUNCTION trigger_cdc();
```

`pg_notify('emp_cdc_events', ...)` does not require pre-creating the channel. The producer simply listens to the same channel name.

---

## Project 2 Destination DB Schema

The destination table stores replicated employee records.

```sql
CREATE TABLE IF NOT EXISTS employees (
    emp_id INT PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    dob DATE,
    city VARCHAR(100),
    salary INT,
    synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

The destination `emp_id` should not be identity-generated. It must reuse the source `emp_id` so update and delete events target the correct row.

---

## Kafka Topics

Create the main CDC topic:

```bash
docker exec -it cdc_kafka kafka-topics \
  --bootstrap-server kafka:9092 \
  --create \
  --topic KafkaCDCProject \
  --partitions 3 \
  --replication-factor 1
```

Create the DLQ topic:

```bash
docker exec -it cdc_kafka kafka-topics \
  --bootstrap-server kafka:9092 \
  --create \
  --topic KafkaCDCProjectDLQ \
  --partitions 3 \
  --replication-factor 1
```

List topics:

```bash
docker exec -it cdc_kafka kafka-topics \
  --bootstrap-server kafka:9092 \
  --list
```

---

## Producer Design

The producer is responsible for:

1. Reading `producer_offset.last_cdc_id`.
2. Fetching `emp_cdc` rows where `cdc_id > last_cdc_id`.
3. Sending CDC events to Kafka.
4. Waiting for Kafka acknowledgement.
5. Updating `producer_offset` only after Kafka send succeeds.
6. Listening for PostgreSQL notifications to reduce polling.

### Producer fetch query

```sql
SELECT
    cdc_id,
    emp_id,
    first_name,
    last_name,
    dob,
    city,
    salary,
    action,
    created_at
FROM emp_cdc
WHERE cdc_id > :last_cdc_id
ORDER BY cdc_id ASC
LIMIT :batch_size;
```

### Producer send flow

```python
for row in rows:
    event = dict(row)

    future = producer.send(
        KAFKA_TOPIC,
        key=event["emp_id"],
        value=event
    )

    future.get(timeout=10)
    max_cdc_id = event["cdc_id"]

producer.flush()

if max_cdc_id is not None:
    upsert_producer_offset(max_cdc_id)
```

### SQLAlchemy with `LISTEN/NOTIFY`

The rest of the project can use SQLAlchemy sessions. For `LISTEN/NOTIFY`, the producer can reuse the SQLAlchemy engine and take a raw DBAPI connection.

```python
import select
import psycopg2.extensions

def listen_for_cdc_events(self) -> None:
    engine = self.connector.get_sql_engine()
    raw_conn = engine.raw_connection()

    dbapi_conn = (
        raw_conn.driver_connection
        if hasattr(raw_conn, "driver_connection")
        else raw_conn.connection
        if hasattr(raw_conn, "connection")
        else raw_conn
    )

    dbapi_conn.set_isolation_level(
        psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT
    )

    cursor = dbapi_conn.cursor()
    cursor.execute("LISTEN emp_cdc_events;")

    print("Listening for PostgreSQL notifications on channel: emp_cdc_events")

    try:
        while True:
            has_notification = select.select([dbapi_conn], [], [], 5)

            if has_notification == ([], [], []):
                print("No notification. Running periodic CDC sweep.")
                self.publish_batch()
                continue

            dbapi_conn.poll()

            while dbapi_conn.notifies:
                notify = dbapi_conn.notifies.pop(0)

                print(
                    f"Received CDC notification: "
                    f"channel={notify.channel}, payload={notify.payload}"
                )

                self.publish_batch()

    finally:
        cursor.close()
        raw_conn.close()
```

The notification payload contains the new `cdc_id`, but the producer should not publish only that one row. It should still call `publish_batch()` and read all rows where `cdc_id > last_cdc_id`. This is safer because notifications can be missed when the producer is down.

---

## Consumer Design

The consumer is responsible for:

1. Reading CDC messages from Kafka.
2. Validating the message.
3. Applying the event to the destination database.
4. Sending invalid messages to DLQ.
5. Committing Kafka offset only after successful DB commit or successful DLQ send.

### Consumer group ID

The consumer should use a fixed `group_id`:

```python
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    group_id="employee_cdc_consumer_group",
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    key_deserializer=lambda k: k.decode("utf-8") if k else None,
)
```

The consumer group does not need to be created manually in Kafka. It is not like a Kafka topic. Kafka automatically recognizes the group when the consumer connects and stores committed offsets in its internal `__consumer_offsets` topic.

If `consumer.commit()` is used without a `group_id`, the client raises:

```text
AssertionError: Requires group_id
```

This happens because Kafka needs to know which consumer group the offset belongs to.

### Consumer apply logic

For `INSERT` and `UPDATE`, use upsert:

```sql
INSERT INTO employees (
    emp_id,
    first_name,
    last_name,
    dob,
    city,
    salary,
    synced_at
)
VALUES (
    :emp_id,
    :first_name,
    :last_name,
    :dob,
    :city,
    :salary,
    NOW()
)
ON CONFLICT (emp_id)
DO UPDATE SET
    first_name = EXCLUDED.first_name,
    last_name = EXCLUDED.last_name,
    dob = EXCLUDED.dob,
    city = EXCLUDED.city,
    salary = EXCLUDED.salary,
    synced_at = NOW();
```

For `DELETE`:

```sql
DELETE FROM employees
WHERE emp_id = :emp_id;
```

### Message validation

Before applying an event, validate required fields.

```python
def validate_event(event):
    required_fields = ["cdc_id", "emp_id", "action"]

    for field in required_fields:
        if field not in event:
            raise ValueError(f"Missing required field: {field}")

    if event["action"] not in ("INSERT", "UPDATE", "DELETE"):
        raise ValueError(f"Invalid action: {event['action']}")

    if int(event["emp_id"]) <= 0:
        raise ValueError("emp_id must be positive")

    if event["action"] in ("INSERT", "UPDATE"):
        for field in ["first_name", "last_name", "dob", "city", "salary"]:
            if field not in event:
                raise ValueError(f"Missing employee field: {field}")

        if event["salary"] is not None and int(event["salary"]) < 0:
            raise ValueError("salary cannot be negative")
```

### DLQ design

Bad messages should be sent to a DLQ topic instead of blocking the consumer forever.

DLQ message example:

```python
dlq_message = {
    "error": str(error),
    "error_type": type(error).__name__,
    "original_event": event,
    "source_topic": message.topic,
    "source_partition": message.partition,
    "source_offset": message.offset,
    "source_key": message.key,
    "failed_at": datetime.utcnow().isoformat(),
}
```

Send to DLQ:

```python
future = dlq_producer.send(
    KAFKA_DLQ_TOPIC,
    key=event.get("emp_id", "unknown"),
    value=dlq_message,
)

future.get(timeout=10)
dlq_producer.flush()
```

Consumer offset commit policy:

| Situation | Action |
|---|---|
| DB update succeeds | Commit Kafka offset |
| Bad message is sent to DLQ | Commit Kafka offset |
| Database/system error | Do not commit offset; retry later |

This prevents invalid messages from blocking the stream while still allowing transient system failures to be retried.

---

## Run Project 2

Set environment variables:

```bash
export KAFKA_BOOTSTRAP_SERVERS="localhost:29092"
export KAFKA_TOPIC="KafkaCDCProject"
export KAFKA_DLQ_TOPIC="KafkaCDCProjectDLQ"

export POSTGRES_HOST="localhost"
export POSTGRES_USER="postgres"
export POSTGRES_PASSWORD="postgres"

export POSTGRES_SRC_DB="source_db"
export POSTGRES_TGT_DB="dest_db"

export POSTGRES_SRC_PORT="5433"
export POSTGRES_TGT_PORT="5434"
```

Create Kafka topics:

```bash
docker exec -it cdc_kafka kafka-topics \
  --bootstrap-server kafka:9092 \
  --create \
  --topic KafkaCDCProject \
  --partitions 3 \
  --replication-factor 1
```

```bash
docker exec -it cdc_kafka kafka-topics \
  --bootstrap-server kafka:9092 \
  --create \
  --topic KafkaCDCProjectDLQ \
  --partitions 3 \
  --replication-factor 1
```

Start the consumer in Terminal 1:

```bash
python -m app.consumer
```

Start the producer in Terminal 2:

```bash
python -m app.producer
```

Insert sample data in Terminal 3:

```bash
docker exec -i cdc_source_db psql -U postgres -d source_db < scripts/insert_samples.sql
```

Apply update/delete/insert changes:

```bash
docker exec -i cdc_source_db psql -U postgres -d source_db < scripts/change_samples.sql
```

Check source CDC events:

```bash
docker exec -it cdc_source_db psql -U postgres -d source_db
```

```sql
SELECT *
FROM emp_cdc
ORDER BY cdc_id;

SELECT *
FROM producer_offset;
```

Check destination replication result:

```bash
docker exec -it cdc_dest_db psql -U postgres -d dest_db
```

```sql
SELECT *
FROM employees
ORDER BY emp_id;
```

Check DLQ messages in Kafka UI:

```text
http://localhost:8085
```

Open topic:

```text
KafkaCDCProjectDLQ
```

---

# Reliability Design

## Producer offset

The producer should update `producer_offset.last_cdc_id` only after Kafka send succeeds.

Correct order:

```text
1. Read last_cdc_id from producer_offset
2. Read emp_cdc rows where cdc_id > last_cdc_id
3. Send CDC events to Kafka
4. Wait for Kafka acknowledgement
5. Flush producer
6. Upsert producer_offset with the max cdc_id sent
```

This prevents data loss. If the producer updates the offset before Kafka accepts the message, a crash could cause a CDC row to be skipped permanently.

## Consumer offset

The consumer should commit Kafka offset only after the destination DB commit succeeds.

Correct order:

```text
1. Consume Kafka message
2. Validate message
3. Apply INSERT / UPDATE / DELETE to destination DB
4. Commit DB transaction
5. Commit Kafka consumer offset
```

If a bad message is sent to DLQ successfully, the consumer can commit the original Kafka offset so the stream can continue.

## Why `emp_cdc` is still needed with `pg_notify`

`pg_notify` is only a signal. It is not durable storage.

If the producer is offline, it can miss notifications. For this reason, `emp_cdc` remains the source of truth. When the producer starts, it first runs `publish_batch()` to catch up on all CDC rows where `cdc_id > last_cdc_id`. Then it listens for new notifications.

---

# Common Troubleshooting

## KafkaTimeoutError on producer send

Usually this means the producer cannot connect to the correct advertised Kafka listener.

If Python runs on the laptop, use:

```text
localhost:29092
```

If Python runs inside Docker, use:

```text
kafka:9092
```

Check Kafka UI:

```text
http://localhost:8085
```

## SQLAlchemy UnboundExecutionError

This means the session is not bound to an engine. Make sure the connector creates an engine and returns it:

```python
def get_sql_engine(self):
    if self.sql_engine is None:
        self.sql_engine = create_engine(self.target_url)
    return self.sql_engine
```

Also make sure `sessionmaker` is bound to the engine:

```python
self.SessionLocal = sessionmaker(bind=self.get_sql_engine())
```

## SQLAlchemy raw SQL ArgumentError

SQLAlchemy 2.x requires raw SQL strings to be wrapped with `text()`:

```python
session.execute(text("SELECT * FROM emp_cdc"))
```

## SQLAlchemy parameter error

Pass parameters as a dictionary:

```python
session.execute(
    text("SELECT * FROM emp_cdc WHERE cdc_id > :last_cdc_id"),
    {"last_cdc_id": last_cdc_id}
)
```

Do not pass bind parameters as keyword arguments.

## Destination insert fails on `emp_id`

The destination `employees.emp_id` should be:

```sql
emp_id INT PRIMARY KEY
```

It should not be `GENERATED ALWAYS AS IDENTITY`, because the destination must reuse the source employee ID.

## Consumer commit fails with `Requires group_id`

This happens when the consumer calls:

```python
consumer.commit()
```

but the consumer was created without a `group_id`.

Fix:

```python
group_id="employee_cdc_consumer_group"
```

The consumer group does not need to be manually created in Kafka. It is created/recognized automatically when the consumer connects.

## Consumer does not read old messages

If the same `group_id` already committed offsets, Kafka will resume from the committed offset. For testing from the beginning, use a new group ID or reset offsets.

Example:

```python
group_id="employee_cdc_consumer_group_test_1"
```

---

# Reset Everything

To reset Docker containers and database state:

```bash
docker compose down -v
```

Start again:

```bash
docker compose up -d
```

---

# Summary

Project 1 demonstrates a batch-style Kafka ETL pipeline from CSV to PostgreSQL.

Project 2 demonstrates a CDC-style Kafka pipeline where PostgreSQL triggers capture source table changes, `LISTEN/NOTIFY` wakes up the producer, Kafka carries CDC events, DLQ isolates bad messages, and the consumer replicates valid changes to a destination PostgreSQL database.
