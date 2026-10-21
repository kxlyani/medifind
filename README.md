# MediFind — Distributed Medicine Availability Finder

**Mini Project, Distributed Systems**

People often visit 4–5 medical stores searching for one medicine because there is no live,
shared view of stock. MediFind lets pharmacies publish stock updates as events and lets users
search a medicine to see the nearest pharmacies that have it, built as containerized
microservices on Docker and Kubernetes, with autoscaling, messaging and caching.

**Full step-by-step explanation: [GUIDE.md](GUIDE.md)**

## Architecture at a glance

```
Simulator ─► Pharmacy Service ─► RabbitMQ ─► Inventory Consumer (×2) ─► PostgreSQL
                                                     │ clears cache          ▲
Browser ─► Frontend (Nginx) ─► Search Service (×2–5, autoscaled) ◄─► Redis  │
                │                       └──── REST on cache miss ─► Inventory Service (×2)
                └──── Reserve ─────────────────────────────────────►   │ RPC lock/unlock
                                                                        ▼
                                          Coordinator cluster: node 1 · node 2 · node 3 ★ leader
                                          (beacons + Bully election + lock manager, all via RPC)
```

| Service | Role | Port |
|---|---|---|
| `frontend` | Three pages (Find medicine, Pharmacy portal, System view) + reverse proxy | 8080 (Compose), 30080 (K8s) |
| `search-service` | Public search API, Redis cache, distance sorting; **autoscaled** | 8002 |
| `inventory-service` | Owns the database; stock queries; reservations under a distributed lock | 8001 |
| `coordinator` (×3) | RPC, beacon protocol, Bully leader election, mutual exclusion (lock manager) | 9000 |
| `inventory-consumer` | Reads events from RabbitMQ, saves to DB, clears cache | — |
| `pharmacy-service` | Accepts stock updates, publishes events | 8003 |
| `simulator` | 20 fake pharmacies sending updates | — |
| PostgreSQL, RabbitMQ, Redis | Database, message broker, cache | 5432, 5672/15672, 6379 |

## Quick start

**Docker Compose (development)**

```bash
docker compose up --build
# Web app:   http://localhost:8080
# RabbitMQ:  http://localhost:15672  (medifind / medifind)
```

**Kubernetes (Minikube)**

```bash
minikube start --cpus=4 --memory=4096
bash scripts/deploy.sh                    # Windows: powershell -ExecutionPolicy Bypass -File scripts\deploy.ps1
minikube service frontend -n medifind
```

**Load test (triggers autoscaling)**

```bash
pip install locust
locust -f loadtest/locustfile.py --host <frontend-url> --headless -u 300 -r 30 -t 3m
kubectl -n medifind get hpa -w            # watch Search scale from 2 to 5 pods
```

**Mutual exclusion race demo**

```bash
python demo/race_test.py --url <frontend-url>    # overbooking without the lock, exact with it
# or press "Run the test" on the System view page
```

## DS concepts demonstrated

**RPC** · **message-oriented communication** · **mutual exclusion** · **beacon protocol** ·
**Bully algorithm** · microservices · many producers · competing consumers ·
horizontal scaling and autoscaling (HPA) · load balancing · service discovery · shared cache
with invalidation · eventual consistency · at-least-once delivery · idempotency ·
last-write-wins ordering · fault tolerance and graceful degradation · self-healing.

See [GUIDE.md](GUIDE.md) §17 for where each one appears in the code, and §22 for the
coordination algorithms.

## Project structure

```
medifind/
├── db/init.sql                 schema + seed data (20 pharmacies, 30 medicines)
├── coordinator/node.py         RPC server, beacon protocol, Bully election, lock manager
├── inventory-service/          main.py (REST API + reservations), consumer.py, coordination.py (RPC client)
├── search-service/             main.py (search + cache)
├── pharmacy-service/           main.py (event producer)
├── simulator/                  simulate.py
├── frontend/                   nginx.conf, site/ (index.html, pharmacy.html, system.html, assets/, fonts/)
├── k8s/                        Kubernetes manifests + HPA
├── scripts/                    deploy.sh, deploy.ps1, teardown.sh
├── loadtest/locustfile.py      load test
├── demo/race_test.py           mutual exclusion demo (race condition vs lock)
├── docker-compose.yml
├── GUIDE.md                    complete explanation, demo script, viva Q&A
└── README.md
```
