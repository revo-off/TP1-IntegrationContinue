import json
import os
import subprocess
import time
import uuid
from typing import Any, Callable, Dict, Generator, Optional

import httpx
import pytest
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

# Configuration depuis les variables d'environnement
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sales.orders")
API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "sales")
POSTGRES_USER = os.getenv("POSTGRES_USER", "sales")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "sales")

RUN_INTEGRATION = os.getenv("RUN_INTEGRATION_TESTS", "true").lower() == "true"


def is_kafka_ready() -> bool:
    """Vérifie si le broker Kafka est joignable."""
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            request_timeout_ms=3000,
        )
        consumer.topics()
        consumer.close()
        return True
    except (KafkaError, Exception):
        return False


# Skip automatique si les tests d'intégration sont désactivés ou Kafka absent
pytestmark = pytest.mark.skipif(
    not RUN_INTEGRATION or not is_kafka_ready(),
    reason="Integration tests disabled or Kafka not available. Start Docker compose.",
)


class PostgresHelper:
    """Gestionnaire de requêtes PostgreSQL avec fallback docker exec."""

    @staticmethod
    def get_order(order_id: str) -> Optional[Dict[str, Any]]:
        # Tentative 1 : psycopg2 direct
        try:
            import psycopg2

            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                dbname=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                connect_timeout=2,
            )
            cur = conn.cursor()
            cur.execute(
                "SELECT row_to_json(t) FROM (SELECT * FROM processed_orders WHERE order_id = %s) t;",
                (order_id,),
            )
            row = cur.fetchone()
            cur.close()
            conn.close()
            if row and row[0]:
                return row[0]
        except Exception:
            pass

        # Tentative 2 : fallback via docker exec (indépendant des conflits de ports locaux)
        sql = f"SELECT row_to_json(t) FROM (SELECT * FROM processed_orders WHERE order_id = '{order_id}') t;"
        cmd = [
            "docker",
            "exec",
            "sales-postgres",
            "psql",
            "-U",
            POSTGRES_USER,
            "-d",
            POSTGRES_DB,
            "-t",
            "-A",
            "-c",
            sql,
        ]
        try:
            res = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
            if res.returncode == 0 and res.stdout.strip():
                return json.loads(res.stdout.strip())
        except Exception:
            pass

        return None

    @classmethod
    def wait_for_order(
        cls, order_id: str, timeout_sec: float = 20.0, poll_interval: float = 0.5
    ) -> Optional[Dict[str, Any]]:
        """Interroge la table processed_orders jusqu'à l'arrivée de la commande ou expiration."""
        start_time = time.time()
        while time.time() - start_time < timeout_sec:
            order = cls.get_order(order_id)
            if order:
                return order
            time.sleep(poll_interval)
        return None

    @staticmethod
    def delete_order(order_id: str) -> None:
        """Supprime une commande de test pour nettoyage."""
        try:
            import psycopg2

            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                dbname=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                connect_timeout=2,
            )
            cur = conn.cursor()
            cur.execute("DELETE FROM processed_orders WHERE order_id = %s;", (order_id,))
            conn.commit()
            cur.close()
            conn.close()
            return
        except Exception:
            pass

        cmd = [
            "docker",
            "exec",
            "sales-postgres",
            "psql",
            "-U",
            POSTGRES_USER,
            "-d",
            POSTGRES_DB,
            "-c",
            f"DELETE FROM processed_orders WHERE order_id = '{order_id}';",
        ]
        try:
            subprocess.run(cmd, capture_output=True, timeout=5)
        except Exception:
            pass


@pytest.fixture
def postgres_helper() -> PostgresHelper:
    """Fixture fournissant l'utilitaire d'interrogation PostgreSQL."""
    return PostgresHelper()


@pytest.fixture
def kafka_producer() -> Generator[KafkaProducer, None, None]:
    """Fixture fournissant un producteur Kafka connecté aux services réels."""
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        request_timeout_ms=10000,
    )
    yield producer
    producer.flush()
    producer.close()


@pytest.fixture
def kafka_consumer_factory() -> Generator[Callable[..., KafkaConsumer], None, None]:
    """Fabrique de consommateurs Kafka isolés avec auto-nettoyage."""
    created_consumers = []

    def _create_consumer(
        topic: str = KAFKA_TOPIC,
        group_id: Optional[str] = None,
        auto_offset_reset: str = "earliest",
        timeout_ms: int = 10000,
    ) -> KafkaConsumer:
        gid = group_id or f"test-group-{uuid.uuid4().hex[:8]}"
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset=auto_offset_reset,
            enable_auto_commit=True,
            group_id=gid,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            consumer_timeout_ms=timeout_ms,
        )
        created_consumers.append(consumer)
        return consumer

    yield _create_consumer

    for c in created_consumers:
        try:
            c.close()
        except Exception:
            pass


class IntegrationApiClient:
    """Client API combinant requêtes HTTP réelles et fallback TestClient."""

    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")
        self._real_http = False
        try:
            r = httpx.get(f"{self.base_url}/api/health", timeout=2.0)
            if r.status_code == 200:
                self._real_http = True
        except Exception:
            self._real_http = False

        if not self._real_http:
            from fastapi.testclient import TestClient
            from app.main import app
            self._test_client = TestClient(app)

    def get(self, path: str):
        if self._real_http:
            return httpx.get(f"{self.base_url}{path}", timeout=10.0)
        return self._test_client.get(path)

    def post(self, path: str, json: Any):
        if self._real_http:
            return httpx.post(f"{self.base_url}{path}", json=json, timeout=10.0)
        return self._test_client.post(path, json=json)


@pytest.fixture
def api_client() -> IntegrationApiClient:
    """Fixture fournissant un client pour l'API Sales."""
    return IntegrationApiClient(API_BASE_URL)
