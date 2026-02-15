from kafka import KafkaConsumer, KafkaProducer
import json
import hashlib
from datetime import datetime
from tokenizer import *
from templateTree import TemplateTree
from cluster import Cluster
from semanticNormalization import *
from behavioralFeatures import *
from temporalFeatures import *

clusters = []

def assign_cluster(tpl):
    for c in clusters:
        if c.try_add(tpl):
            return c
    c = Cluster(tpl)
    clusters.append(c)
    return c

def template_id(tpl):
    return hashlib.md5(" ".join(tpl.tokens).encode()).hexdigest()

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


consumer = KafkaConsumer(
    "logs",
    bootstrap_servers="kafka:9092",
    auto_offset_reset="earliest",
    group_id=None,
)

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


for msg in consumer:
    raw_msg = msg.value.decode(errors="ignore")
    try:
        payload = json.loads(raw_msg)
        log = payload.get("log", raw_msg)
    except json.JSONDecodeError:
        log = raw_msg

    key, tpl, cluster, is_new, gen, tid, features = process(log)

    print("Sending normaized logs to Kafka producer")
    event = {
        "log": log,
        "syntax_key": key,
        "template": " ".join(tpl.tokens),
        "semantic": semantic_mapping(tpl),
        "cluster": str(cluster),
        "is_new": is_new,
        "generalized": gen,
        "features": features
    }

    producer.send("logs_normalized", event)
    producer.flush()