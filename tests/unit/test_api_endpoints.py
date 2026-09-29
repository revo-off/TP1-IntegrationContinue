from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient

from app.main import app, PRODUCTS

client = TestClient(app)


def test_health_endpoint():
    """Vérifie que l'endpoint de santé répond 200 avec le statut UP."""
    response = client.get("/api/health")
    assert response.status_code == 200
    assert response.json() == {"status": "UP", "service": "sales-api"}


def test_products_endpoint():
    """Vérifie que l'endpoint /api/products renvoie le catalogue de produits."""
    response = client.get("/api/products")
    assert response.status_code == 200
    assert response.json() == PRODUCTS


def test_create_order_unknown_product():
    """Vérifie qu'une commande pour un produit inexistant renvoie une erreur 404."""
    payload = {
        "customer_id": "C001",
        "product_id": "UNKNOWN_PROD",
        "quantity": 1,
        "unit_price": 50.0,
    }
    response = client.post("/api/orders", json=payload)
    assert response.status_code == 404
    assert response.json()["detail"] == "Unknown product"


@patch("app.main.create_kafka_producer")
def test_create_order_success(mock_create_producer):
    """Vérifie la création d'une commande valide avec publication mockée dans Kafka."""
    mock_producer = MagicMock()
    mock_future = MagicMock()
    mock_producer.send.return_value = mock_future
    mock_create_producer.return_value = mock_producer

    payload = {
        "customer_id": "C001",
        "product_id": "P001",
        "quantity": 2,
        "unit_price": 1299.90,
    }
    response = client.post("/api/orders", json=payload)

    assert response.status_code == 201
    data = response.json()
    assert data["order_id"].startswith("ORD-")
    assert data["customer_id"] == "C001"
    assert data["product_id"] == "P001"
    assert data["quantity"] == 2
    assert data["unit_price"] == 1299.90
    assert data["total_amount"] == 2599.80
    assert "timestamp" in data

    # Vérifie que KafkaProducer a bien été appelé
    mock_producer.send.assert_called_once()
    mock_future.get.assert_called_once_with(timeout=10)
    mock_producer.flush.assert_called_once()
    mock_producer.close.assert_called_once()


def test_create_order_validation_error():
    """Vérifie qu'un payload invalide (ex: quantité négative) renvoie un statut 422."""
    payload = {
        "customer_id": "C001",
        "product_id": "P001",
        "quantity": -2,
        "unit_price": 10.0,
    }
    response = client.post("/api/orders", json=payload)
    assert response.status_code == 422


@patch("app.main.KafkaProducer")
def test_create_kafka_producer(mock_kafka_producer):
    """Vérifie l'instanciation du producteur Kafka avec les bons paramètres."""
    from app.main import create_kafka_producer
    producer = create_kafka_producer()
    mock_kafka_producer.assert_called_once()
    assert producer == mock_kafka_producer.return_value
