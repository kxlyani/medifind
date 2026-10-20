"""
Load test for MediFind with Locust.

Simulates many users searching for random medicines from random locations,
through the frontend's /api path (the same path a real browser uses).

Install:   pip install locust
Run (with web UI at http://localhost:8089):
    locust -f loadtest/locustfile.py --host http://<frontend-address>
Run headless (no UI), e.g. 200 users for 2 minutes:
    locust -f loadtest/locustfile.py --host http://<frontend-address> \
           --headless -u 200 -r 20 -t 2m

<frontend-address> is what `minikube service frontend -n medifind --url` prints,
or http://localhost:8080 when using Docker Compose.

At the end it prints how many requests each Search pod answered,
which is your proof of load balancing across replicas.
"""
import random
from collections import Counter

from locust import HttpUser, between, events, task

FALLBACK_MEDICINES = ["Dolo 650", "Paracetamol 500mg", "Cetirizine 10mg", "Azithromycin 500mg"]
FALLBACK_AREAS = [{"lat": 18.52, "lng": 73.85}]

served_by = Counter()
sources = Counter()


class SearchUser(HttpUser):
    wait_time = between(0.2, 1.0)   # each user pauses 0.2-1 s between searches

    def on_start(self):
        try:
            self.medicines = self.client.get("/api/medicines", name="/api/medicines").json()
            self.areas = self.client.get("/api/areas", name="/api/areas").json()
        except Exception:
            self.medicines, self.areas = FALLBACK_MEDICINES, FALLBACK_AREAS

    @task
    def search(self):
        area = random.choice(self.areas)
        params = {"medicine": random.choice(self.medicines), "lat": area["lat"], "lng": area["lng"]}
        with self.client.get("/api/search", params=params, name="/api/search",
                             catch_response=True) as resp:
            if resp.status_code != 200:
                resp.failure(f"HTTP {resp.status_code}")
                return
            data = resp.json()
            served_by[data.get("served_by", "?")] += 1
            sources[data.get("source", "?")] += 1
            resp.success()


@events.quitting.add_listener
def print_distribution(environment, **kwargs):
    total = sum(served_by.values()) or 1
    print("\n=== Requests answered per Search pod (load balancing) ===")
    for pod, n in served_by.most_common():
        print(f"  {pod:45s} {n:7d}  ({100 * n / total:4.1f}%)")
    print("=== Cache hit rate ===")
    for src, n in sources.most_common():
        print(f"  {src:25s} {n:7d}  ({100 * n / total:4.1f}%)")
