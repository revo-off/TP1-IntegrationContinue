import re
from datetime import datetime, timezone
import pytest
from pydantic import ValidationError

from app.main import Order, OrderEvent, build_order_event


# 1. Calcul du montant total
def test_total_amount_calculation():
    """Vérifie le calcul correct du montant total et l'arrondi à 2 décimales."""
    order = Order(customer_id="C001", product_id="P001", quantity=3, unit_price=19.99)
    event = build_order_event(order)
    assert event["total_amount"] == 59.97


# 2. Quantité positive
def test_quantity_positive():
    """Vérifie les cas nominaux et limites autorisés pour la quantité (1 à 1000)."""
    assert Order(customer_id="C001", product_id="P001", quantity=1, unit_price=10.0).quantity == 1
    assert Order(customer_id="C001", product_id="P001", quantity=1000, unit_price=10.0).quantity == 1000
    with pytest.raises(ValidationError):
        Order(customer_id="C001", product_id="P001", quantity=1001, unit_price=10.0)


# 3. Quantité nulle
def test_quantity_zero():
    """Vérifie qu'une quantité nulle (0) lève une ValidationError."""
    with pytest.raises(ValidationError):
        Order(customer_id="C001", product_id="P001", quantity=0, unit_price=10.0)


# 4. Quantité négative
def test_quantity_negative():
    """Vérifie qu'une quantité négative lève une ValidationError."""
    with pytest.raises(ValidationError):
        Order(customer_id="C001", product_id="P001", quantity=-1, unit_price=10.0)


# 5. Prix positif
def test_unit_price_positive():
    """Vérifie que les prix unitaires strictement positifs et <= 100000 sont acceptés."""
    assert Order(customer_id="C001", product_id="P001", quantity=1, unit_price=0.01).unit_price == 0.01
    assert Order(customer_id="C001", product_id="P001", quantity=1, unit_price=100000.0).unit_price == 100000.0


# 6. Prix invalide
def test_unit_price_invalid():
    """Vérifie que les prix nuls, négatifs ou au-delà de la limite maximale sont rejetés."""
    for invalid in [0, -1.0, 100000.01, "invalide"]:
        with pytest.raises(ValidationError):
            Order(customer_id="C001", product_id="P001", quantity=1, unit_price=invalid)


# 7. Identifiant client
def test_customer_id():
    """Vérifie la validation de l'identifiant client (min 2 caractères)."""
    assert Order(customer_id="C1", product_id="P001", quantity=1, unit_price=10.0).customer_id == "C1"
    for invalid in ["", "C", None]:
        with pytest.raises(ValidationError):
            Order(customer_id=invalid, product_id="P001", quantity=1, unit_price=10.0)


# 8. Identifiant produit
def test_product_id():
    """Vérifie la validation de l'identifiant produit (min 2 caractères)."""
    assert Order(customer_id="C001", product_id="P1", quantity=1, unit_price=10.0).product_id == "P1"
    for invalid in ["", "P", None]:
        with pytest.raises(ValidationError):
            Order(customer_id="C001", product_id=invalid, quantity=1, unit_price=10.0)


# 9. Génération de l'identifiant de commande
def test_order_id_generation():
    """Vérifie le format ORD-[0-9A-F]{10} et l'unicité de l'identifiant de commande."""
    order = Order(customer_id="C001", product_id="P001", quantity=1, unit_price=10.0)
    event1 = build_order_event(order)
    event2 = build_order_event(order)

    assert re.match(r"^ORD-[0-9A-F]{10}$", event1["order_id"]) is not None
    assert event1["order_id"] != event2["order_id"]


# 10. Construction correcte d’un événement
def test_build_order_event():
    """Vérifie la structure, le timestamp UTC et la conformité du modèle OrderEvent."""
    order = Order(customer_id="C001", product_id="P001", quantity=2, unit_price=49.90)
    event = build_order_event(order)

    assert event["order_id"].startswith("ORD-")
    assert event["customer_id"] == "C001"
    assert event["product_id"] == "P001"
    assert event["quantity"] == 2
    assert event["unit_price"] == 49.90
    assert event["total_amount"] == 99.80

    # Timestamp valide ISO UTC
    ts = datetime.fromisoformat(event["timestamp"])
    assert ts.tzinfo is not None

    # Validation modèle OrderEvent
    event_model = OrderEvent(**event)
    assert event_model.order_id == event["order_id"]
    assert event_model.total_amount == 99.80
