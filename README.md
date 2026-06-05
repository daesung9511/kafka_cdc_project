# Kafka Data Engineering Projects

This repository contains two Kafka-based data engineering projects.

- **Project 1:** CSV ETL pipeline with Kafka and PostgreSQL
- **Project 2:** Change Data Capture (CDC) pipeline with PostgreSQL triggers, Kafka, and PostgreSQL replication

The main goal is to practice producer/consumer design, Kafka messaging, PostgreSQL loading, and basic reliability patterns such as offset tracking and idempotent upserts.

---

## Repository Structure

```text
kafkaProject/
├── app/
│   ├── config.py                  # Shared Kafka/Postgres config for Project 2
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
│   └── kafka_project1.py          # Project 1 single-file producer/consumer app
├── scripts/
│   ├── create_topics.py           # Create Project 2 Kafka topic
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

Start Kafka, ZooKeeper, Kafka UI, source Postgres, and destination Postgres:

```bash
docker compose up -d
```

Check containers:

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

When running Python from your laptop, use Kafka bootstrap server:

```text
localhost:29092
```

When running Python inside another Docker container, use:

```text
kafka:9092
```

---

# Project 1: CSV ETL Pipeline

## Objective

Project 1 builds a simple ETL pipeline:

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

The producer reads `Employee_Salaries.csv`, filters and transforms the data, then sends valid rows to Kafka. The consumer reads Kafka messages, inserts employee records into PostgreSQL, and updates the total salary by department.

## Transformation Rules

The producer applies these rules:

1. Keep only these departments:
   - `ECC`
   - `CIT`
   - `EMS`
2. Include employees hired after `2010-01-01`.
3. Round salary down to an integer using `floor`.
4. Send transformed records to Kafka.

The date rule is implemented as:

```python
if hire_date <= date(2010, 1, 1):
    return None
```

This matches the expected result shown in the project success criteria.

## Project 1 Tables

`department_employee` stores each valid employee row:

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

The consumer updates the total salary using PostgreSQL upsert:

```sql
INSERT INTO department_employee_salary (department, total_salary)
VALUES (:department, :salary)
ON CONFLICT (department)
DO UPDATE SET
    total_salary = department_employee_salary.total_salary + EXCLUDED.total_salary;
```

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

Project 1 is batch-style. If you run the producer multiple times without resetting the tables or changing the consumer group/topic, totals can be counted again. For a clean test, reset the DB tables before rerunning:

```bash
python proj1/kafka_project1.py init-db --reset
```

If Kafka offsets make the consumer skip older messages, use a new topic or a new consumer group.

---

# Project 2: PostgreSQL Trigger CDC Pipeline

## Objective

Project 2 builds a CDC-style replication pipeline between two PostgreSQL databases:

```text
source_db.employees
        |
        | PostgreSQL trigger + function
        v
source_db.emp_cdc
        |
        | Python producer scans new CDC rows
        v
Kafka topic: KafkaCDCProject
        |
        | Python consumer applies events
        v
dest_db.employees
```

Any `INSERT`, `UPDATE`, or `DELETE` on the source `employees` table is captured into the source `emp_cdc` table. The producer reads new CDC rows and publishes them to Kafka. The consumer reads Kafka messages and applies the same change to the destination `employees` table.

## Project 2 Source Tables

`employees` is the source table:

```sql
CREATE TABLE employees (
   emp_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
   first_name VARCHAR(100),
   last_name VARCHAR(100),
   dob DATE,
   city VARCHAR(100),
   salary INT
);
```

`emp_cdc` is the append-only CDC event log:

```sql
CREATE TABLE emp_cdc (
    cdc_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    emp_id INT NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    dob DATE,
    city VARCHAR(100),
    salary INT,
    action VARCHAR(20) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

`producer_offset` tracks how far the producer has read from `emp_cdc`:

```sql
CREATE TABLE producer_offset (
    producer_id INT PRIMARY KEY,
    last_cdc_id BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

## Trigger and Function Design

The trigger function checks `TG_OP` and writes one CDC event for every source row change:

- `INSERT` uses `NEW`
- `UPDATE` uses `NEW`
- `DELETE` uses `OLD`

The trigger is attached to the source `employees` table:

```sql
CREATE OR REPLACE TRIGGER trg_emp_cdc
AFTER INSERT OR UPDATE OR DELETE ON employees
FOR EACH ROW
EXECUTE FUNCTION trigger_cdc();
```

This means every successful row-level insert, update, or delete on `employees` creates a new row in `emp_cdc`.

## Project 2 Destination Table

The destination table uses the source `emp_id` directly. It should not auto-generate its own employee ID:

```sql
CREATE TABLE employees (
   emp_id INT PRIMARY KEY,
   first_name VARCHAR(100),
   last_name VARCHAR(100),
   dob DATE,
   city VARCHAR(100),
   salary INT,
   synced_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);
```

This is important because the consumer must update or delete the same `emp_id` that came from the source database.

## Run Project 2

Set Project 2 environment variables:

```bash
export KAFKA_BOOTSTRAP_SERVER="localhost:29092"
export KAFKA_TOPIC="KafkaCDCProject"
export POSTGRES_HOST="localhost"
export POSTGRES_USER="postgres"
export POSTGRES_PASSWORD="postgres"
export POSTGRES_SRC_DB="source_db"
export POSTGRES_TGT_DB="dest_db"
export POSTGRES_SRC_PORT="5433"
export POSTGRES_TGT_PORT="5434"
```

Create the Kafka topic:

```bash
PYTHONPATH=. python scripts/create_topics.py
```

Start the Project 2 consumer in Terminal 1:

```bash
python -m app.consumer
```

Start the Project 2 producer in Terminal 2:

```bash
python -m app.producer
```

Insert sample source data in Terminal 3:

```bash
docker exec -i cdc_source_db psql -U postgres -d source_db < scripts/insert_samples.sql
```

Apply update/delete/insert changes:

```bash
docker exec -i cdc_source_db psql -U postgres -d source_db < scripts/change_samples.sql
```

Check the source CDC table:

```bash
docker exec -it cdc_source_db psql -U postgres -d source_db
```

```sql
SELECT * FROM emp_cdc ORDER BY cdc_id;
SELECT * FROM producer_offset;
```

Check the destination table:

```bash
docker exec -it cdc_dest_db psql -U postgres -d dest_db
```

```sql
SELECT * FROM employees ORDER BY emp_id;
```

## Project 2 Reliability Design

The producer should update `producer_offset.last_cdc_id` only after Kafka send succeeds.

Correct order:

```text
1. Read last_cdc_id from producer_offset
2. Read emp_cdc rows where cdc_id > last_cdc_id
3. Send each CDC event to Kafka
4. Wait for Kafka acknowledgement
5. Flush producer
6. Upsert producer_offset with the max cdc_id sent
```

This prevents data loss. If the producer updates the offset before Kafka accepts the message, a crash could cause a CDC row to be skipped forever.

The consumer should commit Kafka offset only after the destination DB commit succeeds.

Correct order:

```text
1. Consume Kafka message
2. Validate message
3. Apply INSERT / UPDATE / DELETE to destination DB
4. Commit DB transaction
5. Commit Kafka consumer offset
```

This prevents message loss if the consumer crashes before the database update finishes.

---

# Common Troubleshooting

## KafkaTimeoutError on producer send

Usually this means the producer cannot connect to the correct advertised Kafka listener.

If Python runs on your laptop, use:

```text
localhost:29092
```

If Python runs inside Docker, use:

```text
kafka:9092
```

Check Kafka UI at:

```text
http://localhost:8085
```

## SQLAlchemy UnboundExecutionError

This means the session is not bound to an engine. Make sure the connector creates an engine and passes it into `sessionmaker`:

```python
self.engine = create_engine(database_url)
self.SessionLocal = sessionmaker(bind=self.engine)
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

## Destination insert fails on emp_id

The destination `employees.emp_id` should be:

```sql
emp_id INT PRIMARY KEY
```

It should not be `GENERATED ALWAYS AS IDENTITY`, because the destination must reuse the source employee ID.

---

# Reset Everything

To completely reset Docker containers and database state:

```bash
docker compose down -v
```

Then start again:

```bash
docker compose up -d
```

---

# Summary

Project 1 demonstrates a batch-style Kafka ETL pipeline from CSV to PostgreSQL.

Project 2 demonstrates a CDC-style Kafka pipeline where PostgreSQL triggers capture source table changes and a Kafka producer/consumer pair replicates those changes to another PostgreSQL database.
