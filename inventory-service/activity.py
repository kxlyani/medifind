"""
Activity log, shown live on the System view page.

Every stock update the consumer processes, and every reservation, is pushed
onto a Redis list. The list is shared by all Inventory pods and consumer pods,
so the page shows ONE timeline of what the whole distributed system is doing,
including which pod did each piece of work.

Only the newest MAX_ENTRIES are kept (LPUSH + LTRIM). If Redis is down, the
event is simply not recorded: the log is for watching, never for correctness.
"""
import json
from datetime import datetime, timezone

import redis

KEY = "activity"
MAX_ENTRIES = 60


def record(cache: redis.Redis, kind: str, text: str, detail: str = "", **extra):
    entry = {"time": datetime.now(timezone.utc).isoformat(), "kind": kind,
             "text": text, "detail": detail, **extra}
    try:
        pipe = cache.pipeline()
        pipe.lpush(KEY, json.dumps(entry))
        pipe.ltrim(KEY, 0, MAX_ENTRIES - 1)
        pipe.execute()
    except redis.RedisError:
        pass


def recent(cache: redis.Redis, limit: int = 40) -> list[dict]:
    try:
        return [json.loads(raw) for raw in cache.lrange(KEY, 0, limit - 1)]
    except redis.RedisError:
        return []
