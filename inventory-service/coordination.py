"""
Client for the coordinator cluster (used by the Inventory Service).

It talks to the coordinators with RPC (XML-RPC). Notice how remote calls look
exactly like local function calls:  proxy.acquire(resource, token, 5)
That is RPC's main idea: ACCESS TRANSPARENCY.

How a lock is obtained:
  1. Find the current leader (ask any coordinator: get_leader()).
  2. Call acquire() on the leader. It blocks until the lock is granted
     (or times out) because the leader queues requests in FIFO order.
  3. If we reached a follower by mistake, it answers with a "redirect"
     to the real leader, and we retry there.
  4. If the leader crashed, the RPC fails; we rediscover the new leader
     (elected by the bully algorithm) and try again.
"""
import os
import socket
import time
import uuid
from contextlib import contextmanager
from xmlrpc.client import Error as XMLRPCError
from xmlrpc.client import ServerProxy, Transport

POD_NAME = socket.gethostname()


def parse_peers(text: str) -> dict[int, str]:
    nodes = {}
    for item in text.split(","):
        node_id, addr = item.strip().split("=")
        nodes[int(node_id)] = addr.strip()
    return nodes


PEERS = parse_peers(os.getenv(
    "COORDINATOR_PEERS", "1=coordinator-1:9000,2=coordinator-2:9000,3=coordinator-3:9000"))


class LockUnavailable(Exception):
    pass


class _TimeoutTransport(Transport):
    def __init__(self, timeout: float):
        super().__init__()
        self.timeout = timeout

    def make_connection(self, host):
        conn = super().make_connection(host)
        conn.timeout = self.timeout
        return conn


def _proxy(addr: str, timeout: float) -> ServerProxy:
    return ServerProxy(f"http://{addr}/", transport=_TimeoutTransport(timeout), allow_none=True)


class CoordinatorClient:
    def __init__(self):
        self.leader_addr: str | None = None   # cached so we don't look it up every time

    def find_leader(self) -> str | None:
        for addr in PEERS.values():
            try:
                info = _proxy(addr, 1).get_leader()
                if info["leader_id"]:
                    return info["address"]
            except (OSError, XMLRPCError):
                continue   # that coordinator is down, ask the next one
        return None

    def acquire(self, resource: str, wait: float = 8.0) -> dict:
        """Returns {"token", "address", "leader_id"} once the lock is held."""
        token = f"{POD_NAME}-{uuid.uuid4().hex[:6]}"
        give_up = time.time() + wait + 5
        while time.time() < give_up:
            addr = self.leader_addr or self.find_leader()
            if not addr:
                time.sleep(0.5)          # election probably in progress
                continue
            try:
                result = _proxy(addr, wait + 2).acquire(resource, token, wait)
            except (OSError, XMLRPCError):
                self.leader_addr = None  # leader crashed -> rediscover
                time.sleep(0.3)
                continue
            if result.get("granted"):
                self.leader_addr = addr
                return {"token": token, "address": addr, "leader_id": result["leader_id"]}
            if result.get("reason") == "timeout":
                raise LockUnavailable(f"lock '{resource}' is busy, try again")
            # redirected to the real leader, or leadership changed mid-request
            self.leader_addr = result.get("redirect") or None
            time.sleep(0.1)
        raise LockUnavailable("no coordinator leader available")

    def release(self, resource: str, lock: dict):
        try:
            _proxy(lock["address"], 2).release(resource, lock["token"])
        except (OSError, XMLRPCError):
            pass  # leader is gone; the lock lease expires on its own

    @contextmanager
    def lock(self, resource: str, wait: float = 8.0):
        held = self.acquire(resource, wait)
        try:
            yield held
        finally:
            self.release(resource, held)

    def cluster_status(self) -> list[dict]:
        nodes = []
        for node_id, addr in sorted(PEERS.items()):
            try:
                status = _proxy(addr, 1).status()
                status["up"] = True
            except (OSError, XMLRPCError):
                status = {"node_id": node_id, "address": addr, "up": False, "role": "down"}
            nodes.append(status)
        return nodes
