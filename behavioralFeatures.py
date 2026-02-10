from rules import  *
from collections import Counter
import time
from semanticNormalization import infer_semantic

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