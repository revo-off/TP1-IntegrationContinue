# Déploiement Kubernetes local (Minikube)

```bash
minikube start --driver=docker

# Images locales construites directement dans Minikube
minikube image build -t sales-api:tp3 -f docker/api/Dockerfile .
minikube image build -t sales-spark-streaming:tp3 -f docker/spark/Dockerfile .

# Secret hors Git (non versionné)
kubectl create secret generic postgres-secret --from-literal=POSTGRES_PASSWORD=<mot_de_passe>

kubectl apply -f k8s/
kubectl get pods -w
kubectl port-forward service/sales-api 8000:8000
```

Le mot de passe doit être `sales` si l'on réutilise les valeurs par défaut de `spark/sales_stream.py`;
sinon il est lu depuis le Secret via la variable `POSTGRES_PASSWORD`.
