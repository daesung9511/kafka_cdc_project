from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from app.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_DLQ_TOPIC, KAFKA_TOPIC


def create_topic(admin: KafkaAdminClient, name: str) -> None:
    topic = NewTopic(name=name, num_partitions=3, replication_factor=1)

    try:
        admin.create_topics([topic])
        print(f"Created topic: {name}")
    except TopicAlreadyExistsError:
        print(f"Topic already exists: {name}")


def main() -> None:
    admin = KafkaAdminClient(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        client_id="cdc_admin",
    )

    create_topic(admin, KAFKA_TOPIC)
    create_topic(admin, KAFKA_DLQ_TOPIC)
    admin.close()


if __name__ == "__main__":
    main()
