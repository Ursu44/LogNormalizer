from kafka import KafkaConsumer
import json
from collections import defaultdict, Counter
import re

# ================= REGEX =================
IP = re.compile(r"\d+\.\d+\.\d+\.\d+")
NUMBER = re.compile(r"^\d+$")
ISO_TS = re.compile(r"^\d{4}-\d{2}-\d{2}T")
SYSLOG_TS = re.compile(r"^[A-Z][a-z]{2}\s+\d{1,2}")
URL = re.compile(r"^(https?://)?([a-zA-Z0-9-]+\.)+[a-zA-Z]{2,}")

# ================= TEMPLATE =================
class Template:
    def __init__(self, tokens):
        self.tokens = tokens[:]
        self.count = 1
        self.samples = defaultdict(Counter)

    def update(self, tokens, raw_tokens):
        generalized = False
        new_tokens = []

        for i, (t, s) in enumerate(zip(tokens, self.tokens)):
            if t == s:
                new_tokens.append(s)
            else:
                new_tokens.append("<*>")
                generalized = True

            if new_tokens[-1] == "<*>":
                self.samples[i][raw_tokens[i]] += 1

        self.tokens = new_tokens
        self.count += 1
        return generalized

# ================= TOKENIZATION =================
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

# ================= TEMPLATE TREE =================
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

# ================= CLUSTER =================
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
        result = []
        max_len = max(len(t.tokens) for t in self.templates)
        for i in range(max_len):
            vals = [t.tokens[i] for t in self.templates if i < len(t.tokens)]
            tok, cnt = Counter(vals).most_common(1)[0]
            result.append(tok if cnt / len(vals) > 0.5 else "<*>")
        return " ".join(result)

clusters = []

def assign_cluster(tpl):
    for c in clusters:
        if c.try_add(tpl):
            return c
    c = Cluster(tpl)
    clusters.append(c)
    return c

# ================= SEMANTIC INFERENCE (STEP 2) =================
def normalize_sample(s):
    if "=" in s:
        k, v = s.split("=", 1)
        return k.lower(), v
    return None, s

SEMANTIC_RULES = {
    "<user>": lambda k, v: k in {"user", "username", "account"},
    "<process>": lambda k, v: k == "process" or v.endswith(".exe"),
    "<src_ip>": lambda k, v: k == "src_ip",
    "<ip>": lambda k, v: IP.fullmatch(v) is not None,
    "<path>": lambda k, v: v.startswith("/"),
    "<command>": lambda k, v: k == "command",
    "<request_id>": lambda k, v: k in {"request_id", "req_id"},
    "<url>": lambda k, v: URL.fullmatch(v) is not None,
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
    return votes.most_common(1)[0][0] if votes else "<value>"

# ================= SEMANTIC UNIFICATION (STEP 3) =================
SEMANTIC_CANONICAL = {
    "<user>": "<user>",
    "<account>": "<user>",
    "<username>": "<user>",

    "<src_ip>": "<src_ip>",
    "<ip>": "<src_ip>",

    "<process>": "<process>",
    "<command>": "<process>",

    "<request_id>": "<request_id>",
}

def unify_semantic(label):
    return SEMANTIC_CANONICAL.get(label, label)

def semantic_mapping(tpl):
    result = []
    for i, tok in enumerate(tpl.tokens):
        if tok != "<*>":
            result.append(tok)
        else:
            local = infer_semantic(tpl.samples.get(i, Counter()))
            result.append(unify_semantic(local))
    return " ".join(result)

# ================= ENGINE =================
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
    return key, tpl, cluster, is_new, gen

# ================= KAFKA =================
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

    key, tpl, cluster, is_new, gen = process(log)

    print(log)
    print("syntax_key :", key)
    print("template   :", " ".join(tpl.tokens))
    print("semantic   :", semantic_mapping(tpl))
    print("count      :", tpl.count)
    print("cluster    :", cluster)

    if tpl.samples:
        print("samples:")
        for p, c in tpl.samples.items():
            print(f"  pos {p}: {dict(c)}")

    print("NEW TEMPLATE" if is_new else "GENERALIZED" if gen else "MATCHED")
    print("-" * 80)
