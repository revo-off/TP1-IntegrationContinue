import time
import uuid


def test_api_produces_order_event_to_kafka(api_client, kafka_consumer_factory):
    """Test A — API → Kafka : vérifie qu'une commande envoyée à l'API produit un événement dans sales.orders."""
    consumer = kafka_consumer_factory()
    unique_customer = f"C_{uuid.uuid4().hex[:6]}"

    payload = {
        "customer_id": unique_customer,
        "product_id": "P001",
        "quantity": 2,
        "unit_price": 1299.90,
    }

    # 1. Envoi de la commande à l'API
    response = api_client.post("/api/orders", json=payload)
    assert response.status_code == 201
    order_id = response.json()["order_id"]

    # 2. Vérification de la présence de l'événement dans Kafka
    received_event = None
    start_time = time.time()
    for message in consumer:
        if message.value.get("order_id") == order_id:
            received_event = message.value
            break
        if time.time() - start_time > 10:
            break

    assert received_event is not None, f"L'événement {order_id} n'a pas été reçu dans Kafka."
    assert received_event["order_id"] == order_id
    assert received_event["customer_id"] == unique_customer
    assert received_event["product_id"] == "P001"
    assert received_event["quantity"] == 2
    assert received_event["unit_price"] == 1299.90
    assert received_event["total_amount"] == 2599.80
