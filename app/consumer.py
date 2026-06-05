from typing import Dict, Any

from kafka import KafkaConsumer, KafkaProducer
from sqlalchemy import text

from app.config import POSTGRES_TGT_CONFIG, KAFKA_PRODUCER_CONFIG, KAFKA_CONSUMER_CONFIG, KAFKA_TOPIC
from app.connector import PostgresConnector


class EmployeeCDCConsumer:
    def __init__(self) -> None:
        self.connector = PostgresConnector(**POSTGRES_TGT_CONFIG)

        self.consumer = KafkaConsumer(KAFKA_TOPIC, **KAFKA_CONSUMER_CONFIG)
        self.producer = KafkaProducer(**KAFKA_PRODUCER_CONFIG)

    def validate_message(self, message: dict) -> None:
        if message["emp_id"] is None or message["cdc_id"] is None or message["action"] is None:
            raise ValueError(f"Invalid message: {message}")

        if int(message["emp_id"]) <= 0:
            raise ValueError("emp_id must be positive")

        if message["action"] not in {"INSERT", "UPDATE", "DELETE"}:
            raise ValueError(f"Unknown action: {message['action']}")

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
        for message in self.consumer:
            event = message.value
            print(event)
            try:
                self.validate_message(event)
                self.apply_event(event)
            except Exception as e:
                raise e




if __name__ == "__main__":
    EmployeeCDCConsumer().run()
