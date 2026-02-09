from collections import defaultdict, Counter

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