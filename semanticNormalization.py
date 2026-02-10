from rules import SEMANTIC_RULES, SEMANTIC_CANONICAL
from collections import Counter

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