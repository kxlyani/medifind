#!/usr/bin/env bash
# Deploy MediFind to Minikube (Linux / macOS / Git Bash on Windows).
# Run from the project root:   bash scripts/deploy.sh
set -euo pipefail
cd "$(dirname "$0")/.."

echo "==> 1/5  Checking Minikube is running"
minikube status >/dev/null || { echo "Start it first:  minikube start --cpus=4 --memory=4096"; exit 1; }

echo "==> 2/5  Enabling metrics-server (needed by the autoscaler)"
minikube addons enable metrics-server

echo "==> 3/5  Building images inside Minikube (no Docker Hub push needed)"
for svc in coordinator inventory-service search-service pharmacy-service notification-service simulator frontend; do
  echo "    building medifind/$svc:1.0"
  minikube image build -t "medifind/$svc:1.0" "./$svc"
done

echo "==> 4/5  Applying Kubernetes manifests"
kubectl apply -f k8s/00-namespace-config.yaml
# Turn db/init.sql into a ConfigMap that PostgreSQL runs on first start
kubectl -n medifind create configmap db-init --from-file=init.sql=db/init.sql \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f k8s/

echo "==> 5/5  Waiting for all pods to be ready (first run can take a few minutes)"
kubectl -n medifind wait --for=condition=available deployment --all --timeout=300s
kubectl -n medifind rollout status statefulset/coordinator --timeout=120s

kubectl -n medifind get pods
echo
echo "Done! Open the app with:   minikube service frontend -n medifind"
echo "RabbitMQ dashboard:        kubectl -n medifind port-forward svc/rabbitmq 15672:15672"
echo "                           then http://localhost:15672  (medifind / medifind)"
