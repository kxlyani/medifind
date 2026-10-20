#!/usr/bin/env bash
# Remove everything MediFind created in the cluster (database data included).
# Run from the project root:   bash scripts/teardown.sh
# Windows equivalent:          kubectl delete namespace medifind
set -euo pipefail
kubectl delete namespace medifind --ignore-not-found
echo "MediFind removed. Stop the cluster too with:  minikube stop"
