from collections import Counter

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