import select
import psycopg2.extensions

from kafka import KafkaProducer
from sqlalchemy import text

from app.connector import PostgresConnector
from config import (
    POSTGRES_SRC_CONFIG,
    KAFKA_PRODUCER_CONFIG, KAFKA_TOPIC,
)

class EmployeeCDCProducer:
    def __init__(self) -> None:
        self.connector = PostgresConnector(**POSTGRES_SRC_CONFIG)
        self.producer = KafkaProducer(**KAFKA_PRODUCER_CONFIG)
        self.topic = KAFKA_TOPIC



    def get_latest_cdc_id(self):
        with self.connector.get_session() as session:
            result = session.execute(text("SELECT last_cdc_id FROM producer_offset ORDER BY last_cdc_id DESC LIMIT 1"))

            rows = result.fetchall()
            if len(rows) == 0:
                return 1
            else:
                return rows[0][0]


    def check_cdc(self, last_cdc_id: int) -> bool:

        with self.connector.get_session() as session:
            result = session.execute(text("SELECT * FROM emp_cdc WHERE cdc_id > :last_cdc_id"), {"last_cdc_id":last_cdc_id})
            return result.mappings().all()

    def update_latest_cdc_id(self, last_cdc_id: int):
        with self.connector.get_session() as session:
            result = session.execute(
                text(
                    """
                        INSERT INTO producer_offset (
                           producer_id,
                            last_cdc_id,
                            updated_at
                        )
                        VALUES (
                            :producer_id,
                            :last_cdc_id,
                            NOW()
                        )
                        ON CONFLICT (producer_id)
                        DO UPDATE SET
                            last_cdc_id = EXCLUDED.last_cdc_id,
                           updated_at = NOW();
                    """
                ), {
                "producer_id": 1,
                "last_cdc_id": last_cdc_id,
            })
            session.commit()


    def publish_batch(self):

        last_cdc_id = self.get_latest_cdc_id()
        print(f"Publishing batch: {last_cdc_id}")
        rows = self.check_cdc(last_cdc_id)
        if rows is None:
            print("No cdc")
            return
        max_cdc_id = 0
        for row in rows:
            row = dict(row)
            future = self.producer.send(self.topic, key = row["cdc_id"],value=row)
            print(f"Sent {row}")
            future.get(timeout=10)
            max_cdc_id = row["cdc_id"]
        if max_cdc_id:
            self.update_latest_cdc_id(max_cdc_id)
        self.producer.flush()

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

        print("Listening for Postgres notifications on channel: emp_cdc_events")

        try:
            while True:
                has_notification = select.select([dbapi_conn], [], [], 10)

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

    def run(self) -> None:
        print("Employee CDC producer started.")

        self.publish_batch()
        self.listen_for_cdc_events()





if __name__ == "__main__":
    producer = EmployeeCDCProducer()
    producer.run()











