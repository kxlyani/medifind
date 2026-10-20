"""
Pharmacy Simulator  (MANY PRODUCERS)
------------------------------------
Pretends to be many pharmacies sending stock updates, so the system has
realistic, continuous traffic without real pharmacies.

Settings (environment variables):
  PHARMACY_URL   where the Pharmacy Service is      (default http://pharmacy-service:8003)
  SEARCH_URL     used once to fetch medicine names  (default http://search-service:8002)
  PHARMACIES     how many pharmacies to simulate    (default 20)
  RATE           updates per second, in total       (default 2)
  TOTAL          stop after this many updates; 0 = run forever (default 0)
"""
import os
import random
import time

import httpx

PHARMACY_URL = os.getenv("PHARMACY_URL", "http://pharmacy-service:8003")
SEARCH_URL = os.getenv("SEARCH_URL", "http://search-service:8002")
PHARMACIES = int(os.getenv("PHARMACIES", "20"))
RATE = float(os.getenv("RATE", "2"))
TOTAL = int(os.getenv("TOTAL", "0"))

FALLBACK_MEDICINES = ["Paracetamol 500mg", "Dolo 650", "Azithromycin 500mg",
                      "Cetirizine 10mg", "Metformin 500mg", "Insulin Glargine"]


def load_medicines(client: httpx.Client) -> list[str]:
    for _ in range(15):
        try:
            names = client.get(f"{SEARCH_URL}/medicines").json()
            if names:
                return names
        except Exception as e:
            print(f"[simulator] waiting for search service: {e}", flush=True)
        time.sleep(2)
    print("[simulator] using fallback medicine list", flush=True)
    return FALLBACK_MEDICINES


def random_quantity() -> int:
    # 25% chance a medicine runs out, otherwise a normal restock amount
    return 0 if random.random() < 0.25 else random.randint(1, 60)


def main():
    client = httpx.Client(timeout=5.0)
    medicines = load_medicines(client)
    print(f"[simulator] {PHARMACIES} pharmacies, {len(medicines)} medicines, {RATE}/sec", flush=True)

    sent = failed = 0
    interval = 1.0 / RATE if RATE > 0 else 0
    while TOTAL == 0 or sent + failed < TOTAL:
        update = {
            "pharmacy_id": random.randint(1, PHARMACIES),
            "medicine": random.choice(medicines),
            "quantity": random_quantity(),
        }
        try:
            r = client.post(f"{PHARMACY_URL}/stock", json=update)
            r.raise_for_status()
            sent += 1
            if sent % 10 == 0 or RATE <= 5:
                print(f"[simulator] sent={sent} failed={failed} last={update}", flush=True)
        except Exception as e:
            failed += 1
            print(f"[simulator] send failed ({failed}): {e}", flush=True)
            time.sleep(1)
        if interval:
            time.sleep(interval)
    print(f"[simulator] done. sent={sent} failed={failed}", flush=True)


if __name__ == "__main__":
    main()
