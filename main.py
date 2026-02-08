from kafka import KafkaConsumer
import json
import re
import time
import hashlib
from collections import defaultdict, Counter, deque
from datetime import datetime

# =========================================================
# REGEX
# =========================================================
IP = re.compile(r"\d+\.\d+\.\d+\.\d+")
NUMBER = re.compile(r"^\d+$")
ISO_TS = re.compile(r"^\d{4}-\d{2}-\d{2}T")
SYSLOG_TS = re.compile(r"^[A-Z][a-z]{2}\s+\d{1,2}")
URL = re.compile(r"^(https?://)?([a-zA-Z0-9-]+\.)+[a-zA-Z]{2,}")
FILENAME = re.compile(r".+\.(exe|txt|docx|pptx|pdf|sh|bat|ps1)$")

# =========================================================
# TEMPLATE
# =========================================================
class Template:
    def __init__(self, tokens):
        self.tokens = tokens[:]
        self.count = 1
        self.samples = defaultdict(Counter)

    def update(self, shaped, raw):
        generalized = False
        new_tokens = []

        for i, (a, b) in enumerate(zip(shaped, self.tokens)):
            if a == b:
                new_tokens.append(b)
            else:
                new_tokens.append("<*>")
                generalized = True
                self.samples[i][raw[i]] += 1

        self.tokens = new_tokens
        self.count += 1
        return generalized

# =========================================================
# TOKENIZATION
# =========================================================
def token_shape(tok):
    if IP.fullmatch(tok):
        return "<*>"
    if NUMBER.fullmatch(tok):
        return "<*>"
    if "=" in tok:
        return "<*>"
    if "[" in tok and "]" in tok:
        return "<*>"
    if URL.fullmatch(tok):
        return "<*>"
    if FILENAME.fullmatch(tok):
        return "<filename>"
    return tok

def timestamp_type(log):
    if ISO_TS.match(log):
        return "ISO"
    if SYSLOG_TS.match(log):
        return "SYSLOG"
    return "NONE"

def tokenize(log, shaped=True):
    ts = timestamp_type(log)
    tokens = log.split()

    if ts == "SYSLOG":
        tokens = tokens[3:]
    elif ts == "ISO":
        tokens = tokens[1:]

    return [token_shape(t) if shaped else t for t in tokens]

# =========================================================
# TEMPLATE TREE
# =========================================================
class TemplateNode:
    def __init__(self):
        self.children = {}
        self.wildcard = None
        self.template = None

class TemplateTree:
    def __init__(self):
        self.root = TemplateNode()

    def match_or_insert(self, shaped, raw):
        node = self.root
        for tok in shaped:
            if tok in node.children:
                node = node.children[tok]
            elif node.wildcard:
                node = node.wildcard
            else:
                new = TemplateNode()
                if tok == "<*>":
                    node.wildcard = new
                else:
                    node.children[tok] = new
                node = new
        if node.template:
            gen = node.template.update(shaped, raw)
            return node.template, False, gen
        tpl = Template(shaped)
        for i, t in enumerate(shaped):
            if t == "<*>":
                tpl.samples[i][raw[i]] += 1
        node.template = tpl
        return tpl, True, False

# =========================================================
# CLUSTERING
# =========================================================
class Cluster:
    def __init__(self, tpl):
        self.templates = [tpl]

    def try_add(self, tpl, threshold=0.7):
        base = self.templates[0].tokens
        matches = sum(
            1 for a, b in zip(base, tpl.tokens)
            if a == b or a == "<*>" or b == "<*>"
        )
        if matches / max(len(base), len(tpl.tokens)) >= threshold:
            self.templates.append(tpl)
            return True
        return False

    def __str__(self):
        out = []
        max_len = max(len(t.tokens) for t in self.templates)
        for i in range(max_len):
            vals = [t.tokens[i] for t in self.templates if i < len(t.tokens)]
            tok, cnt = Counter(vals).most_common(1)[0]
            out.append(tok if cnt / len(vals) > 0.5 else "<*>")
        return " ".join(out)

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

SEMANTIC_RULES = {
    "<user>": lambda k, v: k in {"user", "username", "account"},
    "<process>": lambda k, v: k == "process" or v.endswith(".exe"),
    "<src_ip>": lambda k, v: IP.fullmatch(v) is not None,
    "<path>": lambda k, v: v.startswith("/"),
    "<request_id>": lambda k, v: k in {"request_id", "req_id"},
    "<url>": lambda k, v: URL.fullmatch(v) is not None,
    "<filename>": lambda k, v: FILENAME.fullmatch(v) is not None
}

SEMANTIC_CANONICAL = {
    "<ip>": "<src_ip>",
    "<username>": "<user>",
    "<account>": "<user>",
}

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
USER_STATE = defaultdict(lambda: {"timestamps": deque(), "ips": Counter(), "processes": Counter()})
IP_STATE = defaultdict(lambda: {"users": Counter()})
TEMPLATE_TRANSITIONS = defaultdict(Counter)
LAST_TEMPLATE_PER_USER = {}

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
MONTHS = {m: i for i, m in enumerate(["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"],1)}
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
