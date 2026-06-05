from datetime import datetime
from sqlite3 import OperationalError
from typing import Dict, Any

from kafka import KafkaConsumer, KafkaProducer
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from app.config import POSTGRES_TGT_CONFIG, KAFKA_PRODUCER_CONFIG, KAFKA_CONSUMER_CONFIG, KAFKA_TOPIC, KAFKA_DLQ_TOPIC
from app.connector import PostgresConnector


class EmployeeCDCConsumer:
    def __init__(self) -> None:
        self.connector = PostgresConnector(**POSTGRES_TGT_CONFIG)

        self.consumer = KafkaConsumer(KAFKA_TOPIC, **KAFKA_CONSUMER_CONFIG)
        self.dlq_producer = KafkaProducer(**KAFKA_PRODUCER_CONFIG)

    def validate_event(self, event):
        required_fields = [
            "cdc_id",
            "emp_id",
            "action",
        ]

        for field in required_fields:
            if field not in event:
                raise ValueError(f"Missing required field: {field}")

        if event["action"] not in ("INSERT", "UPDATE", "DELETE"):
            raise ValueError(f"Invalid action: {event['action']}")

        if int(event["emp_id"]) <= 0:
            raise ValueError("emp_id must be positive")

        if event["action"] in ("INSERT", "UPDATE"):
            employee_fields = [
                "first_name",
                "last_name",
                "dob",
                "city",
                "salary",
            ]

            for field in employee_fields:
                if field not in event:
                    raise ValueError(f"Missing employee field: {field}")

            if event["salary"] is not None and int(event["salary"]) < 0:
                raise ValueError("salary cannot be negative")

    def send_to_dlq(self, message, event, error):
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

        key = event.get("emp_id", "unknown") if isinstance(event, dict) else "unknown"

        future = self.dlq_producer.send(
            KAFKA_DLQ_TOPIC,
            key=key,
            value=dlq_message,
        )

        future.get(timeout=10)
        self.dlq_producer.flush()

        print(
            f"Sent message to DLQ: "
            f"topic={message.topic}, "
            f"partition={message.partition}, "
            f"offset={message.offset}, "
            f"error={error}"
        )

    def apply_event(self, message: dict) -> None:
        if message["action"] == "INSERT" or message["action"] == "UPDATE":
            print(f"{message['cdc_id']} message: {message['action']}")
            self.upsert_message(message)

        elif message["action"] == "DELETE":
            print(f"Deleting message: {message['emp_id']}")
            self.delete_employee(message)

    def upsert_message(self, event: Dict[str, Any]) -> None:
        with self.connector.get_session() as session:
            session.execute( text("""
                INSERT INTO employees (emp_id, first_name, last_name, dob, city, salary, synced_at)
                VALUES (:emp_id, :first_name, :last_name, :dob, :city, :salary, CURRENT_TIMESTAMP)
                ON CONFLICT (emp_id)
                DO UPDATE SET
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    dob = EXCLUDED.dob,
                    city = EXCLUDED.city,
                    salary = EXCLUDED.salary,
                    synced_at = CURRENT_TIMESTAMP
                """),
                {
                    "emp_id": event["emp_id"],
                    "first_name": event.get("first_name"),
                    "last_name": event.get("last_name"),
                    "dob": event.get("dob"),
                    "city": event.get("city"),
                    "salary": event.get("salary"),
                },)
            session.commit()

    def delete_employee(self, event: Dict[str, Any]) -> None:
        with self.connector.get_session() as session:
            session.execute(text(
                "DELETE FROM employees WHERE emp_id = :emp_id"),
                {"emp_id": event["emp_id"],},
            )
            session.commit()

    def run(self):
        print("Employee CDC consumer started.")

        for message in self.consumer:
            event = message.value

            try:
                self.validate_event(event)
                self.apply_event(event)

                self.consumer.commit()

                print(
                    f"Applied event: "
                    f"cdc_id={event.get('cdc_id')}, "
                    f"emp_id={event.get('emp_id')}, "
                    f"action={event.get('action')}"
                )

            except (OperationalError, DBAPIError) as error:
                print(f"Database/system error. Will retry later. error={error}")
                break

            except ValueError as error:
                self.send_to_dlq(message, event, error)
                self.consumer.commit()

            except Exception as error:
                self.send_to_dlq(message, event, error)
                self.consumer.commit()



if __name__ == "__main__":
    EmployeeCDCConsumer().run()
