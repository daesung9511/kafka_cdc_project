from datetime import date, datetime
import os
import json
from typing import Any


def json_serializer(obj: Any) -> str:
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} is not JSON serializable")

KAFKA_BOOTSTRAP_SERVER = os.environ.get("KAFKA_BOOTSTRAP_SERVER", "localhost:29092")
KAFKA_PRODUCER_CONFIG = {
    "bootstrap_servers": KAFKA_BOOTSTRAP_SERVER,
    "value_serializer": lambda v: json.dumps(v, default=json_serializer).encode("utf-8"),
    "key_serializer": lambda k: str(k).encode("utf-8"),
    "retries": 5,
    "linger_ms": 10,
}
KAFKA_CONSUMER_CONFIG = {
    "bootstrap_servers": KAFKA_BOOTSTRAP_SERVER,
    "value_deserializer": lambda v: json.loads(v.decode("utf-8")),
    "key_deserializer": lambda k: k.decode("utf-8") if k else None,
    "group_id": "employee_cdc_consumer_group",
}

KAFKA_TOPIC = "KafkaCDCProject"
KAFKA_DLQ_TOPIC = "emp_cdc_dlq"



POSTGRES_CONFIG = {
    'host': os.environ.get('POSTGRES_HOST', 'localhost'),
    'port': os.environ.get('POSTGRES_PORT', '5432'),
    'db_name': os.environ.get('POSTGRES_DB', 'postgres'),
    'user': os.environ.get('POSTGRES_USER', 'postgres'),
    'password': os.environ.get('POSTGRES_PASSWORD', 'postgres'),
}
POSTGRES_SRC_PORT = 5433
POSTGRES_TGT_PORT = 5434
POSTGRESQL_SRC_URL: str = f"postgresql+psycopg2://{POSTGRES_CONFIG['user']}:{POSTGRES_CONFIG['password']}@{POSTGRES_CONFIG['host']}:{POSTGRES_SRC_PORT}/{POSTGRES_CONFIG['db_name']}"
POSTGRESQL_TGT_URL: str = f"postgresql+psycopg2://{POSTGRES_CONFIG['user']}:{POSTGRES_CONFIG['password']}@{POSTGRES_CONFIG['host']}:{POSTGRES_TGT_PORT}/{POSTGRES_CONFIG['db_name']}"

POSTGRES_SRC_CONFIG = {
    **POSTGRES_CONFIG,
    'port': 5433,
    "target_url": POSTGRESQL_SRC_URL,
}

POSTGRES_TGT_CONFIG = {
    **POSTGRES_CONFIG,
    "target_url": POSTGRESQL_TGT_URL,
}
