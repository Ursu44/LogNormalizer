import time
from rules import TEMPORAL_STATE

def update_temporal(tid, now=None):
    now = now or time.time()
    s = TEMPORAL_STATE[tid]
    s["timestamps"].append(now)
    if s["first_seen"] is None:
        s["first_seen"] = now
    s["last_seen"] = now
    cutoff = now - 900
    while s["timestamps"] and s["timestamps"][0] < cutoff:
        s["timestamps"].popleft()
    return s

def temporal_features(state, now=None):
    now = now or time.time()
    def count(sec):
        return sum(1 for t in state["timestamps"] if t >= now - sec)
    rate_1m = count(60)
    rate_5m = count(300)
    rate_15m = len(state["timestamps"])
    return {
        "rate_1m": rate_1m,
        "rate_5m": rate_5m,
        "rate_15m": rate_15m,
        "burst": 1 if rate_1m > 10 and rate_5m > 20 else 0,
        "is_rare": 1 if rate_15m < 5 else 0,
        "first_seen": int(state["first_seen"]),
        "last_seen": int(state["last_seen"]),
    }
