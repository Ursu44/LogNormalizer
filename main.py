from kafka import KafkaConsumer
import json
import time
import hashlib
from datetime import datetime
from tokenizer import *
from templateTree import TemplateTree
from cluster import Cluster


clusters = []

def assign_cluster(tpl):
    for c in clusters:
        if c.try_add(tpl):
            return c
    c = Cluster(tpl)
    clusters.append(c)
    return c

# =========================================================
# SEMANTIC NORMALIZATION
# =========================================================
def normalize_sample(s):
    if "=" in s:
        k, v = s.split("=", 1)
        return k.lower(), v
    return None, s


def infer_semantic(samples):
    votes = Counter()
    for raw, cnt in samples.items():
        k, v = normalize_sample(raw)
        for label, rule in SEMANTIC_RULES.items():
            try:
                if rule(k, v):
                    votes[label] += cnt
            except:
                pass
    if not votes:
        return "<value>"
    top = votes.most_common(1)[0][0]
    return SEMANTIC_CANONICAL.get(top, top)

def semantic_mapping(tpl):
    out = []
    for i, tok in enumerate(tpl.tokens):
        if tok not in {"<*>", "<filename>"}:
            out.append(tok)
        else:
            out.append(infer_semantic(tpl.samples.get(i, Counter())))
    return " ".join(out)

# =========================================================
# ETAPA 3.1 – STRUCTURAL FEATURES
# =========================================================
def template_id(tpl):
    return hashlib.md5(" ".join(tpl.tokens).encode()).hexdigest()

# =========================================================
# ETAPA 3.2 – TEMPORAL FEATURES
# =========================================================
TEMPORAL_STATE = defaultdict(lambda: {"timestamps": deque(), "first_seen": None, "last_seen": None})

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

# =========================================================
# ETAPA 3.3 – BEHAVIORAL FEATURES
# =========================================================

def extract_entities(tpl):
    entities = []
    for i, tok in enumerate(tpl.tokens):
        if tok in {"<*>", "<filename>"}:
            entities.append(infer_semantic(tpl.samples.get(i, Counter())))
    return entities

def transition_prob(user, tid):
    prev = LAST_TEMPLATE_PER_USER.get(user)
    if prev:
        total = sum(TEMPLATE_TRANSITIONS[prev].values())
        return TEMPLATE_TRANSITIONS[prev][tid] / total if total > 0 else 0
    return 0

def time_since_last_template(user, now=None):
    now = now or time.time()
    prev = LAST_TEMPLATE_PER_USER.get(user)
    if prev and prev in TEMPORAL_STATE:
        return now - TEMPORAL_STATE[prev]["last_seen"]
    return None

def first_seen_flags(user, ip):
    return {
        "is_first_seen_for_user": 1 if len(USER_STATE[user]["timestamps"]) == 1 else 0,
        "is_first_seen_for_ip": 1 if ip and sum(IP_STATE[ip]["users"].values()) == 1 else 0
    }

def file_diversity(user, tpl):
    files = [infer_semantic(tpl.samples.get(i, Counter()))
             for i, tok in enumerate(tpl.tokens) if tok == "<filename>"]
    return len(set(files))

def behavioral_features(tpl, tid, now=None):
    now = now or time.time()
    entities = extract_entities(tpl)
    user = next((e for e in entities if e == "<user>"), None)
    ip = next((e for e in entities if e == "<src_ip>"), None)
    process = next((e for e in entities if e == "<process>"), None)
    feats = {}

    if user:
        us = USER_STATE[user]
        us["timestamps"].append(now)
        while us["timestamps"] and us["timestamps"][0] < now - 300:
            us["timestamps"].popleft()
        if ip: us["ips"][ip] += 1
        if process: us["processes"][process] += 1
        feats.update({
            "user_rate_5m": len(us["timestamps"]),
            "user_ip_diversity": len(us["ips"]),
            "new_process_for_user": 1 if process and us["processes"][process] == 1 else 0,
            "file_diversity": file_diversity(user, tpl)
        })
    if ip and user:
        IP_STATE[ip]["users"][user] += 1
        feats["ip_user_diversity"] = len(IP_STATE[ip]["users"])
    prev = LAST_TEMPLATE_PER_USER.get(user)
    if prev:
        TEMPLATE_TRANSITIONS[prev][tid] += 1
        feats["has_prev_template"] = 1
        feats["transition_prob"] = transition_prob(user, tid)
        feats["time_since_last_template"] = time_since_last_template(user, now)
    else:
        feats["has_prev_template"] = 0
        feats["transition_prob"] = 0
        feats["time_since_last_template"] = None
    LAST_TEMPLATE_PER_USER[user] = tid
    if user and ip:
        feats.update(first_seen_flags(user, ip))
    return feats

# =========================================================
# TIMESTAMP PARSING
# =========================================================
def parse_timestamp(log):
    ts_type = timestamp_type(log)
    if ts_type == "ISO":
        try:
            return datetime.fromisoformat(log.split()[0]).timestamp()
        except:
            return time.time()
    elif ts_type == "SYSLOG":
        try:
            tokens = log.split()
            mon, day, hms = tokens[:3]
            dt_str = f"{datetime.now().year}-{MONTHS[mon]:02d}-{int(day):02d} {hms}"
            dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
            return dt.timestamp()
        except:
            return time.time()
    return time.time()

# =========================================================
# ENGINE
# =========================================================
trees = defaultdict(TemplateTree)

def syntax_key(log, depth=6):
    ts = timestamp_type(log)
    shaped = tokenize(log, shaped=True)[:depth]
    return ts + "|" + "|".join(shaped)

def process(log):
    shaped = tokenize(log, shaped=True)
    raw = tokenize(log, shaped=False)
    key = syntax_key(log)
    tpl, is_new, gen = trees[key].match_or_insert(shaped, raw)
    cluster = assign_cluster(tpl)
    tid = template_id(tpl)
    state = update_temporal(tid)
    temporal = temporal_features(state)
    behavioral = behavioral_features(tpl, tid)
    ts_numeric = parse_timestamp(log)
    features = {**temporal, **behavioral, "timestamp": ts_numeric, "template_id": tid, "cluster": str(cluster)}
    return key, tpl, cluster, is_new, gen, tid, features

# =========================================================
# KAFKA
# =========================================================
consumer = KafkaConsumer(
    "logs",
    bootstrap_servers="localhost:29092",
    auto_offset_reset="earliest",
    group_id=None,
)

print("\n📡 Listening for logs...\n")

for msg in consumer:
    raw_msg = msg.value.decode(errors="ignore")
    try:
        payload = json.loads(raw_msg)
        log = payload.get("log", raw_msg)
    except json.JSONDecodeError:
        log = raw_msg

    key, tpl, cluster, is_new, gen, tid, features = process(log)

    # Gata de ML
    print(json.dumps({
        "log": log,
        "syntax_key": key,
        "template": " ".join(tpl.tokens),
        "semantic": semantic_mapping(tpl),
        "cluster": str(cluster),
        "is_new": is_new,
        "generalized": gen,
        "features": features
    }, indent=2))
