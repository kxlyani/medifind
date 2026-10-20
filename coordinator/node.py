"""
Coordinator Node
================
Three copies of this program form a small cluster. Together they show four
classic Distributed Systems algorithms:

1. RPC (Remote Procedure Call)
   Every message between nodes is an RPC using Python's built-in XML-RPC.
   Calling another node looks like a local function call:
       proxy.election(MY_ID)          # actually runs on another machine
   The library MARSHALS the arguments into XML, sends them over HTTP, the
   server UNMARSHALS them, runs the function, and sends the result back.

2. Beacon protocol (failure detection)
   The leader sends a BEACON ("I'm alive, I'm still the leader") to every
   other node every BEACON_INTERVAL seconds. A follower that hears nothing for
   BEACON_TIMEOUT seconds assumes the leader has crashed.

3. Bully algorithm (leader election)
   Each node has a numeric ID; the highest alive ID must become leader.
     * A node that notices the leader is gone sends ELECTION to all HIGHER nodes.
     * Any higher node that is alive replies OK and starts its own election.
     * If nobody higher answers, the node declares itself leader and sends
       COORDINATOR to everyone.
     * When a higher node comes back, it "bullies" its way to leadership.

4. Mutual exclusion (centralized algorithm)
   The current leader acts as the LOCK MANAGER. A client asks
   acquire("stock:3:dolo 650"). If nobody holds that lock it is GRANTED;
   otherwise the request waits in a FIFO QUEUE. release() hands the lock to the
   next waiter. Each lock has a LEASE, so a crashed holder can't block forever.

Configuration (environment variables):
  NODE_ID   numeric ID (optional on Kubernetes: taken from pod name coordinator-N -> N+1)
  PEERS     all nodes, e.g. "1=coordinator-1:9000,2=coordinator-2:9000,3=coordinator-3:9000"
  PORT      port to listen on (default 9000)
"""
import os
import socket
import sys
import threading
import time
from collections import defaultdict, deque
from socketserver import ThreadingMixIn
from xmlrpc.client import ServerProxy, Transport
from xmlrpc.server import SimpleXMLRPCServer

# ---------------------------------------------------------------- settings
BEACON_INTERVAL = float(os.getenv("BEACON_INTERVAL", "2"))   # leader sends a beacon every 2 s
BEACON_TIMEOUT = float(os.getenv("BEACON_TIMEOUT", "6"))     # 3 missed beacons -> leader is dead
ELECTION_WAIT = float(os.getenv("ELECTION_WAIT", "4"))       # how long to wait for COORDINATOR
RPC_TIMEOUT = float(os.getenv("RPC_TIMEOUT", "1"))           # an unanswered RPC = node is down
LOCK_LEASE = float(os.getenv("LOCK_LEASE", "5"))             # max time a lock can be held
PORT = int(os.getenv("PORT", "9000"))


def detect_node_id() -> int:
    if os.getenv("NODE_ID"):
        return int(os.environ["NODE_ID"])
    host = socket.gethostname()               # StatefulSet pods are named coordinator-0, -1, -2
    try:
        return int(host.rsplit("-", 1)[1]) + 1
    except (IndexError, ValueError):
        sys.exit(f"Cannot work out NODE_ID from hostname '{host}'; set NODE_ID")


def parse_peers(text: str) -> dict[int, str]:
    nodes = {}
    for item in text.split(","):
        node_id, addr = item.strip().split("=")
        nodes[int(node_id)] = addr.strip()
    return nodes


MY_ID = detect_node_id()
ALL_NODES = parse_peers(os.getenv(
    "PEERS", "1=localhost:9001,2=localhost:9002,3=localhost:9003"))
PEERS = {i: a for i, a in ALL_NODES.items() if i != MY_ID}


# ---------------------------------------------------------------- RPC client
class TimeoutTransport(Transport):
    """XML-RPC transport with a timeout, so a dead node doesn't hang us."""

    def __init__(self, timeout: float):
        super().__init__()
        self.timeout = timeout

    def make_connection(self, host):
        conn = super().make_connection(host)
        conn.timeout = self.timeout
        return conn


def rpc(node_id: int, method: str, *args, timeout: float = RPC_TIMEOUT):
    """Call `method(*args)` on another node. Raises an exception if it is down."""
    proxy = ServerProxy(f"http://{ALL_NODES[node_id]}/",
                        transport=TimeoutTransport(timeout), allow_none=True)
    return getattr(proxy, method)(*args)


# ---------------------------------------------------------------- mutual exclusion
class MutexManager:
    """Centralized mutual exclusion: one coordinator grants locks in FIFO order."""

    def __init__(self, log):
        self.log = log
        self.cv = threading.Condition()
        self.holders: dict[str, tuple[str, float]] = {}   # resource -> (client, lease expiry)
        self.queues: dict[str, deque] = defaultdict(deque)
        self.generation = 0                                # bumps when the table is reset
        self.stats = {"granted": 0, "had_to_wait": 0, "timeouts": 0, "lease_expired": 0}

    def _expire(self, resource):
        holder = self.holders.get(resource)
        if holder and holder[1] < time.time():
            self.log(f"LOCK lease expired for {holder[0]} on '{resource}', force-releasing")
            del self.holders[resource]
            self.stats["lease_expired"] += 1

    def acquire(self, resource: str, client: str, wait_timeout: float) -> dict:
        with self.cv:
            generation = self.generation
            queue = self.queues[resource]
            queue.append(client)
            if resource in self.holders or queue[0] != client:
                self.stats["had_to_wait"] += 1
                self.log(f"LOCK REQUEST '{resource}' from {client}: busy, queued at position {len(queue)}")
            deadline = time.time() + wait_timeout
            while True:
                if self.generation != generation or client not in queue:
                    return {"granted": False, "reason": "leader changed"}
                self._expire(resource)
                if resource not in self.holders and queue[0] == client:
                    queue.popleft()
                    self.holders[resource] = (client, time.time() + LOCK_LEASE)
                    self.stats["granted"] += 1
                    self.log(f"LOCK GRANTED  '{resource}' -> {client}")
                    return {"granted": True, "leader_id": MY_ID}
                remaining = deadline - time.time()
                if remaining <= 0:
                    queue.remove(client)
                    self.stats["timeouts"] += 1
                    self.cv.notify_all()
                    return {"granted": False, "reason": "timeout"}
                self.cv.wait(min(remaining, 0.5))

    def release(self, resource: str, client: str) -> bool:
        with self.cv:
            holder = self.holders.get(resource)
            if holder and holder[0] == client:
                del self.holders[resource]
                self.log(f"LOCK RELEASED '{resource}' by {client}")
                self.cv.notify_all()      # wake the next waiter in the queue
                return True
            return False

    def reset(self, why: str):
        with self.cv:
            if self.holders or any(self.queues.values()):
                self.log(f"lock table cleared ({why})")
            self.holders.clear()
            self.queues.clear()
            self.generation += 1
            self.cv.notify_all()

    def snapshot(self) -> dict:
        with self.cv:
            now = time.time()
            return {
                "held": {r: {"holder": c, "lease_left_s": round(exp - now, 1)}
                         for r, (c, exp) in self.holders.items()},
                "waiting": {r: len(q) for r, q in self.queues.items() if q},
                "stats": dict(self.stats),
            }


# ---------------------------------------------------------------- the node
class Node:
    def __init__(self):
        self.state = threading.RLock()
        self.leader_id: int | None = None
        self.leader_since = 0.0
        self.last_beacon = 0.0
        self.election_running = False
        self.beacon_seq = 0
        self.started = time.time()
        self.stats = {"elections_started": 0, "times_became_leader": 0,
                      "beacons_sent": 0, "beacons_received": 0}
        self.mutex = MutexManager(self.log)

    # ---------- helpers
    def is_leader(self) -> bool:
        return self.leader_id == MY_ID

    def log(self, msg: str):
        role = "LEADER" if self.is_leader() else "follower"
        print(f"{time.strftime('%H:%M:%S')} [node {MY_ID} {role}] {msg}", flush=True)

    def spawn(self, fn, *args):
        threading.Thread(target=fn, args=args, daemon=True).start()

    # ---------- Bully algorithm
    def start_election(self):
        with self.state:
            if self.election_running:
                return
            self.election_running = True
            self.stats["elections_started"] += 1
        try:
            while True:
                started = time.time()
                higher = sorted(p for p in PEERS if p > MY_ID)
                if higher:
                    self.log(f"BULLY: starting election, sending ELECTION to higher nodes {higher}")
                else:
                    self.log("BULLY: starting election, I have the highest ID")

                answered = []
                for peer in higher:
                    try:
                        if rpc(peer, "election", MY_ID) == "OK":
                            answered.append(peer)
                            self.log(f"BULLY: node {peer} answered OK (it is alive and higher)")
                    except Exception:
                        self.log(f"BULLY: node {peer} did not answer (down)")

                if not answered:
                    self.become_leader()
                    return

                # A higher node is alive and will take over; wait for its COORDINATOR message.
                deadline = time.time() + ELECTION_WAIT
                while time.time() < deadline:
                    with self.state:
                        if self.leader_id and self.leader_id > MY_ID and self.leader_since >= started:
                            return
                    time.sleep(0.2)
                self.log("BULLY: no COORDINATOR message arrived in time, restarting election")
        finally:
            with self.state:
                self.election_running = False

    def become_leader(self):
        with self.state:
            self.leader_id = MY_ID
            self.leader_since = self.last_beacon = time.time()
            self.stats["times_became_leader"] += 1
        self.mutex.reset("new leader starts with an empty lock table")
        self.log("*** BULLY: nobody higher is alive -> I am the new COORDINATOR (leader) ***")
        for peer in PEERS:
            self.spawn(self._announce, peer)

    def _announce(self, peer):
        try:
            rpc(peer, "coordinator", MY_ID)
        except Exception:
            pass  # that node is down; it will learn the leader from beacons later

    # ---------- Beacon protocol
    def beacon_loop(self):
        time.sleep(1.5)                     # give the other nodes a moment to start
        self.spawn(self.start_election)     # every node runs an election when it (re)starts
        while True:
            time.sleep(BEACON_INTERVAL)
            if self.is_leader():
                self.send_beacons()
            else:
                age = time.time() - self.last_beacon
                if (self.leader_id is None or age > BEACON_TIMEOUT) and not self.election_running:
                    if self.leader_id is not None:
                        self.log(f"BEACON: no beacon from leader {self.leader_id} for {age:.1f}s "
                                 f"-> leader presumed DEAD")
                    self.spawn(self.start_election)

    def send_beacons(self):
        self.beacon_seq += 1
        reached, lock = [], threading.Lock()

        def send(peer):
            try:
                rpc(peer, "beacon", MY_ID, self.beacon_seq)
                with lock:
                    reached.append(peer)
            except Exception:
                pass

        threads = [threading.Thread(target=send, args=(p,)) for p in PEERS]
        for t in threads:
            t.start()
        for t in threads:
            t.join(RPC_TIMEOUT + 0.5)
        self.stats["beacons_sent"] += 1
        self.log(f"BEACON #{self.beacon_seq} sent, alive followers: {sorted(reached) or 'none'}")

    # ---------- RPC handlers (called BY other nodes / clients)
    def rpc_election(self, sender: int) -> str:
        self.log(f"BULLY: received ELECTION from node {sender} -> reply OK and take over")
        self.spawn(self.start_election)
        return "OK"

    def rpc_coordinator(self, leader: int) -> bool:
        if leader < MY_ID:
            self.log(f"BULLY: node {leader} claims leadership but my ID is higher -> challenging")
            self.spawn(self.start_election)
            return False
        with self.state:
            was_leader = self.is_leader()
            self.leader_id = leader
            self.leader_since = self.last_beacon = time.time()
        if was_leader and leader != MY_ID:
            self.mutex.reset("stepped down")
        self.log(f"BULLY: received COORDINATOR -> node {leader} is the leader")
        return True

    def rpc_beacon(self, leader: int, seq: int) -> bool:
        if leader < MY_ID:
            # A lower node is acting as leader while I am alive: bully it.
            self.spawn(self.start_election)
            return False
        with self.state:
            if self.leader_id != leader:
                self.log(f"BEACON: now following leader {leader}")
                if self.is_leader():
                    self.mutex.reset("stepped down")
                self.leader_id = leader
                self.leader_since = time.time()
            self.last_beacon = time.time()
            self.stats["beacons_received"] += 1
        return True

    def rpc_get_leader(self) -> dict:
        leader = self.leader_id or 0
        return {"leader_id": leader, "address": ALL_NODES.get(leader, "")}

    def rpc_acquire(self, resource: str, client: str, wait_timeout: float) -> dict:
        if not self.is_leader():
            leader = self.leader_id or 0
            return {"granted": False, "redirect": ALL_NODES.get(leader, ""), "leader_id": leader}
        return self.mutex.acquire(resource, client, float(wait_timeout))

    def rpc_release(self, resource: str, client: str) -> bool:
        return self.is_leader() and self.mutex.release(resource, client)

    def rpc_status(self) -> dict:
        return {
            "node_id": MY_ID,
            "address": ALL_NODES.get(MY_ID, ""),
            "role": "leader" if self.is_leader() else "follower",
            "leader_id": self.leader_id or 0,
            "seconds_since_beacon": None if self.is_leader() else round(time.time() - self.last_beacon, 1),
            "election_running": self.election_running,
            "uptime_s": round(time.time() - self.started),
            "stats": dict(self.stats),
            "locks": self.mutex.snapshot() if self.is_leader() else None,
        }


class ThreadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    """Handle each RPC in its own thread (a waiting lock request must not block beacons)."""
    daemon_threads = True
    request_queue_size = 128


def main():
    node = Node()
    server = ThreadedXMLRPCServer(("0.0.0.0", PORT), allow_none=True, logRequests=False)
    # The functions other nodes / clients can call remotely:
    server.register_function(lambda: True, "ping")
    server.register_function(node.rpc_election, "election")
    server.register_function(node.rpc_coordinator, "coordinator")
    server.register_function(node.rpc_beacon, "beacon")
    server.register_function(node.rpc_get_leader, "get_leader")
    server.register_function(node.rpc_acquire, "acquire")
    server.register_function(node.rpc_release, "release")
    server.register_function(node.rpc_status, "status")

    node.log(f"started on port {PORT}; peers: {PEERS}")
    threading.Thread(target=node.beacon_loop, daemon=True).start()
    server.serve_forever()


if __name__ == "__main__":
    main()
