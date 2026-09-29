import uuid
from datetime import datetime, timezone


def test_kafka_to_spark_to_postgres(kafka_producer, postgres_helper):
    """Test B — Kafka → Spark → PostgreSQL : vérifie qu'un événement Kafka est traité par Spark et sauvé en BDD."""
    order_id = f"ORD-{uuid.uuid4().hex[:10].upper()}"
    customer_id = f"C_SPARK_{uuid.uuid4().hex[:6]}"

    event = {
        "order_id": order_id,
        "customer_id": customer_id,
        "product_id": "P001",
        "quantity": 2,
        "unit_price": 1299.90,
        "total_amount": 2599.80,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    # 1. Publication directe dans Kafka
    kafka_producer.send("sales.orders", value=event).get(timeout=10)
    kafka_producer.flush()

    # 2. Attente de la consommation Spark et de l'enregistrement PostgreSQL
    row = postgres_helper.wait_for_order(order_id, timeout_sec=15.0, poll_interval=0.5)

    # 3. Vérifications des données enregistrées
    assert row is not None, f"La commande {order_id} n'a pas été enregistrée dans PostgreSQL par Spark."
    assert row["order_id"] == order_id
    assert row["customer_id"] == customer_id
    assert row["product_id"] == "P001"
    assert row["quantity"] == 2
    assert float(row["unit_price"]) == 1299.90
    assert float(row["total_amount"]) == 2599.80
    assert "event_timestamp" in row
    assert "processed_at" in row
