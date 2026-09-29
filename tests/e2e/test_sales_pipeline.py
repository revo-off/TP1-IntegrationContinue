import json
import os
import subprocess
import time
from typing import Any, Dict, Optional

import httpx
import pytest

RUN_E2E = os.getenv("RUN_E2E_TESTS", "true").lower() == "true"
API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "sales")
POSTGRES_USER = os.getenv("POSTGRES_USER", "sales")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "sales")

pytestmark = pytest.mark.skipif(
    not RUN_E2E,
    reason="E2E tests disabled. Set RUN_E2E_TESTS=true."
)


def get_order_from_postgres(order_id: str) -> Optional[Dict[str, Any]]:
    """Récupère une commande depuis PostgreSQL (via psycopg2 ou docker exec)."""
    # 1. Tentative psycopg2 directe
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

    # 2. Fallback docker exec
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


def poll_order_in_postgres(order_id: str, timeout_sec: float = 15.0, interval: float = 0.5) -> Optional[Dict[str, Any]]:
    """Attend la persistance de la commande par polling sans attente fixe arbitraire."""
    start_time = time.time()
    while time.time() - start_time < timeout_sec:
        order = get_order_from_postgres(order_id)
        if order:
            return order
        time.sleep(interval)
    return None


def test_e2e_order_pipeline():
    """Scénario E2E complet : API -> Kafka -> PySpark -> PostgreSQL.

    Given : L'infrastructure Docker est démarrée.
    When  : Une commande est envoyée à l'API.
    Then  : La commande est publiée dans Kafka, traitée par Spark, et enregistrée dans PostgreSQL.
    """
    payload = {
        "customer_id": "C100",
        "product_id": "P001",
        "quantity": 3,
        "unit_price": 100.0,
    }

    # 1. Envoi de la commande à l'API
    try:
        res = httpx.post(f"{API_BASE_URL}/api/orders", json=payload, timeout=10.0)
    except Exception:
        from fastapi.testclient import TestClient
        from app.main import app
        res = TestClient(app).post("/api/orders", json=payload)

    assert res.status_code == 201, f"Erreur API : {res.text}"
    api_order = res.json()
    order_id = api_order["order_id"]
    assert order_id.startswith("ORD-")
    assert api_order["total_amount"] == 300.0

    # 2. Attente active (polling avec timeout) du traitement Spark -> PostgreSQL
    processed_order = poll_order_in_postgres(order_id, timeout_sec=15.0, interval=0.5)

    assert processed_order is not None, (
        f"Délai maximal dépassé (15s) : la commande {order_id} n'a pas été trouvée dans PostgreSQL."
    )

    # 3. Vérification des valeurs enregistrées dans PostgreSQL
    assert processed_order["order_id"] == order_id
    assert processed_order["customer_id"] == "C100"
    assert processed_order["product_id"] == "P001"
    assert processed_order["quantity"] == 3
    assert float(processed_order["unit_price"]) == 100.0
    assert float(processed_order["total_amount"]) == 300.0
    assert "event_timestamp" in processed_order
    assert "processed_at" in processed_order
