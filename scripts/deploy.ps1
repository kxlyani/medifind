# Deploy MediFind to Minikube (Windows PowerShell).
# Run from the project root:   powershell -ExecutionPolicy Bypass -File scripts\deploy.ps1
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

Write-Host "==> 1/5  Checking Minikube is running"
minikube status | Out-Null
if ($LASTEXITCODE -ne 0) { Write-Host "Start it first:  minikube start --cpus=4 --memory=4096"; exit 1 }

Write-Host "==> 2/5  Enabling metrics-server (needed by the autoscaler)"
minikube addons enable metrics-server

Write-Host "==> 3/5  Building images inside Minikube (no Docker Hub push needed)"
foreach ($svc in "coordinator", "inventory-service", "search-service", "pharmacy-service", "simulator", "frontend") {
    Write-Host "    building medifind/${svc}:1.0"
    minikube image build -t "medifind/${svc}:1.0" ".\$svc"
    if ($LASTEXITCODE -ne 0) { throw "Build failed for $svc" }
}

Write-Host "==> 4/5  Applying Kubernetes manifests"
kubectl apply -f k8s\00-namespace-config.yaml
kubectl -n medifind create configmap db-init --from-file=init.sql=db\init.sql --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f k8s\

Write-Host "==> 5/5  Waiting for all pods to be ready (first run can take a few minutes)"
kubectl -n medifind wait --for=condition=available deployment --all --timeout=300s
kubectl -n medifind rollout status statefulset/coordinator --timeout=120s

kubectl -n medifind get pods
Write-Host ""
Write-Host "Done! Open the app with:   minikube service frontend -n medifind"
Write-Host "RabbitMQ dashboard:        kubectl -n medifind port-forward svc/rabbitmq 15672:15672"
Write-Host "                           then http://localhost:15672  (medifind / medifind)"
