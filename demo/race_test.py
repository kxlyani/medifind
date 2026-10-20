"""
Mutual exclusion demo: what happens when many people reserve the LAST few
strips of a medicine at exactly the same moment?

Round 1 - WITHOUT the distributed lock (?use_lock=false)
          -> race condition: more reservations succeed than there was stock (overbooking)
Round 2 - WITH the distributed lock (coordinator leader grants it)
          -> exactly as many reservations succeed as there was stock

Uses only the Python standard library. Run from the project folder:

  Docker Compose:   python demo/race_test.py --url http://localhost:8080
  Kubernetes:       python demo/race_test.py --url <output of: minikube service frontend -n medifind --url>

Tip: pause the simulator first so it doesn't change the same stock mid-test
  Compose:     docker compose stop simulator
  Kubernetes:  kubectl -n medifind scale deploy simulator --replicas=0
"""
import argparse
import json
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from concurrent.futures import ThreadPoolExecutor


def http(method: str, url: str, body: dict | None = None, timeout: float = 30):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read() or b"null")
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read() or b"null")
        except ValueError:
            return e.code, None


def current_stock(base: str, pharmacy_id: int, medicine: str) -> int:
    qs = urllib.parse.urlencode({"medicine": medicine, "lat": 0, "lng": 0, "limit": 50})
    status, data = http("GET", f"{base}/api/search?{qs}")
    if status != 200:
        raise SystemExit(f"Search failed (HTTP {status}): {data}")
    for r in data["results"]:
        if r["pharmacy_id"] == pharmacy_id:
            return r["quantity"]
    return 0  # pharmacies with 0 stock are not listed


def set_stock(base: str, pharmacy_id: int, medicine: str, qty: int):
    """Goes through the normal event pipeline: Pharmacy Service -> RabbitMQ -> consumer."""
    status, data = http("POST", f"{base}/pharmacy/stock",
                        {"pharmacy_id": pharmacy_id, "medicine": medicine, "quantity": qty})
    if status != 202:
        raise SystemExit(f"Could not set stock (HTTP {status}): {data}")
    for _ in range(50):
        if current_stock(base, pharmacy_id, medicine) == qty:
            return
        time.sleep(0.3)
    raise SystemExit("Stock update did not arrive. Is the inventory consumer running?")


def run_round(base, pharmacy_id, medicine, stock, n_requests, use_lock):
    set_stock(base, pharmacy_id, medicine, stock)
    start = threading.Barrier(n_requests)   # release all requests at the same instant

    def one(i):
        start.wait()
        return http("POST", f"{base}/inventory/reserve?use_lock={str(use_lock).lower()}",
                    {"pharmacy_id": pharmacy_id, "medicine": medicine, "quantity": 1,
                     "customer": f"user{i}"})

    t0 = time.time()
    with ThreadPoolExecutor(max_workers=n_requests) as pool:
        results = list(pool.map(one, range(n_requests)))
    elapsed = time.time() - t0

    codes = Counter(code for code, _ in results)
    succeeded = codes.get(200, 0)
    time.sleep(0.5)
    final = current_stock(base, pharmacy_id, medicine)
    nodes = Counter(d["lock"].get("granted_by_node") for c, d in results if c == 200 and d["lock"]["used"])
    pods = Counter(d["served_by"] for c, d in results if c == 200)
    return {"succeeded": succeeded, "rejected": codes.get(409, 0),
            "errors": n_requests - succeeded - codes.get(409, 0),
            "final": final, "elapsed": elapsed, "nodes": dict(nodes), "pods": dict(pods)}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--url", default="http://localhost:8080", help="frontend address")
    ap.add_argument("--pharmacy", type=int, default=1)
    ap.add_argument("--medicine", default="Insulin Glargine")
    ap.add_argument("--stock", type=int, default=5, help="units in stock before the rush")
    ap.add_argument("--requests", type=int, default=20, help="simultaneous customers")
    args = ap.parse_args()
    base = args.url.rstrip("/")

    print(f"\n{args.requests} customers try to reserve 1 x '{args.medicine}' at pharmacy "
          f"{args.pharmacy}, which has only {args.stock} in stock.\n")
    rows = []
    for use_lock in (False, True):
        label = "WITH lock (mutual exclusion)" if use_lock else "WITHOUT lock"
        print(f"Running round: {label} ...", flush=True)
        r = run_round(base, args.pharmacy, args.medicine, args.stock, args.requests, use_lock)
        rows.append((label, r))

    print(f"\n{'Round':32s} {'Succeeded':>9s} {'Sold out':>9s} {'Errors':>7s} {'Stock after':>12s} {'Overbooked':>11s}")
    print("-" * 85)
    for label, r in rows:
        over = max(0, r["succeeded"] - args.stock)
        print(f"{label:32s} {r['succeeded']:9d} {r['rejected']:9d} {r['errors']:7d} "
              f"{r['final']:12d} {over:11d}")
    print()
    no_lock, with_lock = rows[0][1], rows[1][1]
    if no_lock["succeeded"] > args.stock:
        print(f"WITHOUT lock: {no_lock['succeeded']} reservations succeeded for only {args.stock} units "
              f"-> RACE CONDITION, {no_lock['succeeded'] - args.stock} customers were promised medicine that doesn't exist.")
    else:
        print("WITHOUT lock: no overbooking this time (races are timing-dependent; run again or add --requests 40).")
    if with_lock["succeeded"] == args.stock and with_lock["final"] == 0:
        print(f"WITH lock:    exactly {args.stock} succeeded and stock ended at 0 -> mutual exclusion works.")
    else:
        print(f"WITH lock:    unexpected result {with_lock}; check the coordinator logs.")
    print(f"WITH lock:    locks granted by coordinator node(s) {with_lock['nodes']}; "
          f"handled by inventory pod(s) {list(with_lock['pods'])}; took {with_lock['elapsed']:.2f}s "
          f"(vs {no_lock['elapsed']:.2f}s without lock: safety costs some speed).\n")


if __name__ == "__main__":
    main()
