# TP1 — Intégration Continue : Rapport de Projet

**Projet** : Real-Time Sales DevOps  
**Auteur** : Théo M.  
**Date** : 29 septembre 2026  
**Repository** : [revo-off/TP1-IntegrationContinue](https://github.com/revo-off/TP1-IntegrationContinue)

---

## 1. Architecture du projet

### Vue d'ensemble

Le projet implémente une plateforme de ventes en temps réel avec pipeline de données complet :

```
Client HTTP ──▶ API FastAPI ──▶ Kafka (sales.orders) ──▶ Spark Streaming ──▶ PostgreSQL
```

### Composants

| Composant | Technologie | Port |
|-----------|-------------|------|
| API | FastAPI (Python 3.11) | 8000 |
| Broker | Apache Kafka 7.6 | 9092 |
| Streaming | Apache Spark 3.5.7 | — |
| Base de données | PostgreSQL 16 | 5432 |
| CI/CD | Jenkins LTS (JDK17) | 8082 |
| Qualité | SonarQube Community | 9000 |

### Structure

```
TP1-IntegrationContinue/
├── app/main.py                    # API FastAPI
├── spark/sales_stream.py          # Job Spark Streaming
├── tests/
│   ├── unit/                      # 16 tests unitaires
│   ├── integration/               # 2 tests d'intégration
│   └── e2e/                       # 1 test E2E
├── docker/                        # Dockerfiles (API, Jenkins, Spark)
├── infra/postgres/init.sql        # Schéma BDD
├── docker-compose.yml             # Orchestration des services
├── Jenkinsfile                    # Pipeline CI/CD
└── sonar-project.properties       # Configuration SonarQube
```

---

## 2. Stratégie de tests

La stratégie suit la pyramide de tests :

```
        ┌──────┐
        │ E2E  │   1 test
       ┌┴──────┴┐
       │ Integ. │   2 tests
      ┌┴────────┴┐
      │  Unit    │  16 tests
      └──────────┘
```

- **Tests unitaires** : logique métier isolée, mocks Kafka, pas de dépendance externe
- **Tests d'intégration** : interactions entre services réels (Docker)
- **Tests E2E** : validation du pipeline complet de bout en bout
- **Couverture** : mesurée via `pytest-cov`, rapport XML exporté vers SonarQube

---

## 3. Tests unitaires réalisés

### Modèles et logique métier — `test_orders.py` (10 tests)

| Test | Ce qui est vérifié |
|------|--------------------|
| `test_total_amount_calculation` | Calcul `qty × price` avec arrondi 2 décimales |
| `test_quantity_positive` | Bornes 1–1000 acceptées, 1001 rejeté |
| `test_quantity_zero` | Quantité 0 → `ValidationError` |
| `test_quantity_negative` | Quantité < 0 → `ValidationError` |
| `test_unit_price_positive` | Prix entre 0.01 et 100000 accepté |
| `test_unit_price_invalid` | Prix 0, négatif, > 100000, texte → rejeté |
| `test_customer_id` | ID client min 2 caractères, vide/None rejeté |
| `test_product_id` | ID produit min 2 caractères, vide/None rejeté |
| `test_order_id_generation` | Format `ORD-[0-9A-F]{10}` et unicité |
| `test_build_order_event` | Structure complète, timestamp UTC, conformité modèle |

### Endpoints API — `test_api_endpoints.py` (6 tests)

| Test | Ce qui est vérifié |
|------|--------------------|
| `test_health_endpoint` | `GET /api/health` → 200, `{"status": "UP"}` |
| `test_products_endpoint` | `GET /api/products` → catalogue complet |
| `test_create_order_unknown_product` | Produit inexistant → 404 |
| `test_create_order_success` | Commande valide → 201 (Kafka mocké) |
| `test_create_order_validation_error` | Payload invalide → 422 |
| `test_create_kafka_producer` | Instanciation correcte du producteur |

---

## 4. Tests d'intégration réalisés

### Test A — API → Kafka (`test_api_kafka.py`)

Vérifie qu'une commande envoyée à l'API produit un événement dans le topic Kafka `sales.orders` :

1. `POST /api/orders` avec un `customer_id` unique
2. Consommation du topic Kafka
3. Vérification des champs de l'événement reçu

### Test B — Kafka → Spark → PostgreSQL (`test_spark_postgres.py`)

Vérifie que Spark traite les événements Kafka et les persiste en BDD :

1. Publication directe d'un événement dans Kafka
2. Polling de PostgreSQL (15s max)
3. Vérification des données dans la table `processed_orders`

---

## 5. Scénario E2E

### Test du pipeline complet (`test_sales_pipeline.py`)

```
POST /api/orders ──▶ API ──▶ Kafka ──▶ Spark ──▶ PostgreSQL
```

| Étape | Action | Vérification |
|-------|--------|-------------|
| **Given** | Infrastructure Docker démarrée | — |
| **When** | `POST /api/orders` (client C100, produit P001, qty=3, prix=100) | Status 201, `total_amount == 300.0` |
| **Then** | Polling PostgreSQL (15s) | Commande retrouvée avec les 7 champs corrects |

Le test inclut des fallbacks automatiques (TestClient si API inaccessible, `docker exec` si psycopg2 indisponible).

---

## 6. Pipeline Jenkins

### Stages

```
Checkout → Environment → Install → Unit Tests → Integration Tests → Build → E2E Tests → SonarQube → Quality Gate
```

| Stage | Description |
|-------|-------------|
| **Checkout** | Récupération du code depuis GitHub |
| **Environment** | Vérification Python et Docker |
| **Install** | `pip install -r requirements.txt` |
| **Unit Tests** | `pytest tests/unit` avec couverture XML |
| **Integration Tests** | `pytest tests/integration` |
| **Build** | `docker build` des images API et Spark |
| **E2E Tests** | `pytest tests/e2e` |
| **SonarQube** | Analyse statique via `withSonarQubeEnv` |
| **Quality Gate** | `waitForQualityGate abortPipeline: true` |

### Post-actions

- Publication des résultats JUnit (`test-results.xml`)
- Archivage du rapport de couverture (`coverage.xml`)

---

## 7. Configuration SonarQube

### `sonar-project.properties`

```properties
sonar.host.url=http://sonarqube:9000
sonar.projectKey=real-time-sales-devops
sonar.projectName=Real-Time Sales DevOps TP
sonar.sources=app
sonar.tests=tests
sonar.python.version=3.11
sonar.python.coverage.reportPaths=coverage.xml
sonar.exclusions=**/__pycache__/**,**/.pytest_cache/**
```

### Intégration Jenkins

- **Serveur** : `SonarQube` → `http://sonarqube:9000` + token d'authentification
- **Outil** : `SonarScanner` avec installation automatique
- **Webhook** : `http://sales-jenkins:8080/sonarqube-webhook/` pour notifier Jenkins

---

## 8. Quality Gate

### Critères

| Métrique | Condition | Seuil |
|----------|-----------|-------|
| Coverage | ≥ | 80 % |
| Bugs | = | 0 |

### Fonctionnement

```
SonarQube analyse ──▶ Quality Gate ──▶ PASS ──▶ Pipeline OK
                                   ──▶ FAIL ──▶ Pipeline STOP
```

Si le Quality Gate échoue, le pipeline Jenkins est immédiatement interrompu (`abortPipeline: true`).

---

## 9. Résultats obtenus

### Tests

| Catégorie | Nombre | Résultat |
|-----------|--------|----------|
| Tests unitaires | 16 | ✅ Passés |
| Tests d'intégration | 2 | ✅ Passés |
| Tests E2E | 1 | ✅ Passé |
| **Total** | **19** | **✅ 19/19** |

### Couverture

```
app/__init__.py    0/0    100%
app/main.py       41/41   100%
TOTAL              41/41   100%
```

### SonarQube

| Métrique | Résultat | Quality Gate |
|----------|----------|-------------|
| Coverage | 100 % | ✅ ≥ 80% |
| Bugs | 0 | ✅ = 0 |
| Vulnerabilities | 0 | ✅ = 0 |
| Duplications | 0 % | ✅ ≤ 3% |

**Quality Gate : PASSED ✅**
