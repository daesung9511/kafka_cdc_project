#!/usr/bin/env python3
"""
Kafka Project 1 - single file version

Project flow:
CSV -> Producer -> Kafka -> Consumer -> Postgres

Requirements:
    pip install pandas kafka-python sqlalchemy psycopg2-binary

Example usage:
    # 1. Create tables
    python kafka_project1_single_file.py init-db

    # 2. Start consumer in one terminal
    python kafka_project1_single_file.py consume

    # 3. Run producer in another terminal
    python kafka_project1_single_file.py produce --csv Employee_Salaries.csv

    # 4. Check expected totals from CSV only
    python kafka_project1_single_file.py expected --csv Employee_Salaries.csv

Environment variables:
    KAFKA_BOOTSTRAP_SERVERS default: localhost:29092
    KAFKA_TOPIC             default: employee_salaries
    DATABASE_URL            default: postgresql+psycopg2://postgres:postgres@localhost:5432/postgres
"""

import argparse
import json
import math
import os
from datetime import date, datetime
from typing import Any, Dict, Iterable, List

import pandas as pd
from kafka import KafkaConsumer, KafkaProducer
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker


# =========================
# Config
# =========================

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "employee_salaries")

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql+psycopg2://postgres:postgres@localhost:5433/postgres",
)

ALLOWED_DEPARTMENTS = {"ECC", "CIT", "EMS"}


def json_serializer(obj: Any) -> str:
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} is not JSON serializable")


# =========================
# Database connector
# =========================

class PostgresConnector:
    def __init__(self, database_url: str) -> None:
        self.engine = create_engine(database_url)
        self.SessionLocal = sessionmaker(
            bind=self.engine,
            autocommit=False,
            autoflush=False,
        )

    def get_session(self):
        return self.SessionLocal()


# =========================
# Schema
# =========================

def init_db(reset: bool = False) -> None:
    connector = PostgresConnector(DATABASE_URL)

    with connector.get_session() as session:
        if reset:
            session.execute(text("DROP TABLE IF EXISTS department_employee"))
            session.execute(text("DROP TABLE IF EXISTS department_employee_salary"))

        session.execute(text("""
            CREATE TABLE IF NOT EXISTS department_employee (
                department VARCHAR(200),
                department_division VARCHAR(200),
                position_title VARCHAR(200),
                hire_date DATE,
                salary INT
            )
        """))

        session.execute(text("""
            CREATE TABLE IF NOT EXISTS department_employee_salary (
                department VARCHAR(200) PRIMARY KEY,
                total_salary BIGINT
            )
        """))

        session.commit()

    print("Postgres tables are ready.")


# =========================
# Transform logic
# =========================

def parse_hire_date(value: Any):
    if pd.isna(value):
        return None

    parsed = pd.to_datetime(value, errors="coerce")

    if pd.isna(parsed):
        return None

    return parsed.date()


def transform_row(row: Dict[str, Any]):
    department = row.get("Department")

    if department not in ALLOWED_DEPARTMENTS:
        return None

    hire_date = parse_hire_date(row.get("Initial Hire Date"))

    if hire_date is None:
        return None

    # Employees hired after 2010 means hire date must be 2011-01-01 or later.
    from datetime import date

    if hire_date <= date(2010, 1, 1):
        return None

    salary_raw = row.get("Salary")

    if pd.isna(salary_raw):
        return None

    salary = math.floor(float(salary_raw))

    return {
        "department": department,
        "department_division": row.get("Department Division"),
        "position_title": row.get("Position Title"),
        "hire_date": hire_date.isoformat(),
        "salary": salary,
    }


def read_and_transform_csv(csv_path: str) -> List[Dict[str, Any]]:
    df = pd.read_csv(csv_path)

    events = []

    for _, row in df.iterrows():
        event = transform_row(row.to_dict())

        if event is not None:
            events.append(event)

    return events


# =========================
# Producer
# =========================

class EmployeeSalaryProducer:
    def __init__(self) -> None:
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v, default=json_serializer).encode("utf-8"),
            key_serializer=lambda k: str(k).encode("utf-8"),
            retries=5,
            linger_ms=10,
        )

    def send_events(self, events: Iterable[Dict[str, Any]]) -> None:
        count = 0

        for event in events:
            future = self.producer.send(
                KAFKA_TOPIC,
                key=event["department"],
                value=event,
            )

            # Wait for broker acknowledgement so we know the message was accepted.
            future.get(timeout=10)

            count += 1

        self.producer.flush()
        print(f"Sent {count} messages to Kafka topic '{KAFKA_TOPIC}'.")

    def close(self) -> None:
        self.producer.close()


def run_producer(csv_path: str) -> None:
    events = read_and_transform_csv(csv_path)
    print(f"Transformed {len(events)} valid rows from CSV.")

    producer = EmployeeSalaryProducer()

    try:
        producer.send_events(events)
    finally:
        producer.close()


# =========================
# Consumer
# =========================

class EmployeeSalaryConsumer:
    def __init__(self) -> None:
        self.connector = PostgresConnector(DATABASE_URL)

        self.consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id="employee_salary_consumer_group",
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
        )

    def insert_department_employee(self, session, event: Dict[str, Any]) -> None:
        session.execute(
            text("""
                INSERT INTO department_employee (
                    department,
                    department_division,
                    position_title,
                    hire_date,
                    salary
                )
                VALUES (
                    :department,
                    :department_division,
                    :position_title,
                    :hire_date,
                    :salary
                )
            """),
            {
                "department": event["department"],
                "department_division": event["department_division"],
                "position_title": event["position_title"],
                "hire_date": event["hire_date"],
                "salary": event["salary"],
            },
        )

    def upsert_department_total(self, session, event: Dict[str, Any]) -> None:
        session.execute(
            text("""
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
                    total_salary = department_employee_salary.total_salary + EXCLUDED.total_salary
            """),
            {
                "department": event["department"],
                "salary": event["salary"],
            },
        )

    def apply_event(self, event: Dict[str, Any]) -> None:
        with self.connector.get_session() as session:
            try:
                self.insert_department_employee(session, event)
                self.upsert_department_total(session, event)
                session.commit()
            except Exception:
                session.rollback()
                raise

    def run(self) -> None:
        print(f"Consumer started. topic={KAFKA_TOPIC}, bootstrap={KAFKA_BOOTSTRAP_SERVERS}")

        for message in self.consumer:
            event = message.value

            try:
                self.apply_event(event)

                # Commit Kafka offset only after DB commit succeeds.
                self.consumer.commit()

                print(
                    f"Loaded event: department={event['department']}, "
                    f"salary={event['salary']}, title={event['position_title']}"
                )

            except Exception as exc:
                print(f"Consumer failed for event={event}. error={exc}")
                # Do not commit here. Without DLQ, this allows retry on restart.

    def close(self) -> None:
        self.consumer.close()


def run_consumer() -> None:
    consumer = EmployeeSalaryConsumer()

    try:
        consumer.run()
    finally:
        consumer.close()


# =========================
# Validation helper
# =========================

def print_expected_totals(csv_path: str) -> None:
    events = read_and_transform_csv(csv_path)

    totals: Dict[str, int] = {}

    for event in events:
        department = event["department"]
        salary = event["salary"]
        totals[department] = totals.get(department, 0) + salary

    print("Expected totals from CSV transformation:")
    for department in sorted(totals):
        print(f"{department}: {totals[department]:,}")


def print_db_totals() -> None:
    connector = PostgresConnector(DATABASE_URL)

    with connector.get_session() as session:
        result = session.execute(text("""
            SELECT department, total_salary
            FROM department_employee_salary
            ORDER BY department
        """))

        rows = result.fetchall()

    print("Current DB totals:")
    for department, total_salary in rows:
        print(f"{department}: {total_salary:,}")


# =========================
# CLI
# =========================

def main() -> None:
    parser = argparse.ArgumentParser(description="Kafka Project 1 single-file app")
    subparsers = parser.add_subparsers(dest="command", required=True)

    init_parser = subparsers.add_parser("init-db")
    init_parser.add_argument("--reset", action="store_true")

    produce_parser = subparsers.add_parser("produce")
    produce_parser.add_argument("--csv", required=True)

    subparsers.add_parser("consume")

    expected_parser = subparsers.add_parser("expected")
    expected_parser.add_argument("--csv", required=True)

    subparsers.add_parser("db-totals")

    args = parser.parse_args()

    if args.command == "init-db":
        init_db(reset=args.reset)

    elif args.command == "produce":
        run_producer(args.csv)

    elif args.command == "consume":
        run_consumer()

    elif args.command == "expected":
        print_expected_totals(args.csv)

    elif args.command == "db-totals":
        print_db_totals()


if __name__ == "__main__":
    main()
