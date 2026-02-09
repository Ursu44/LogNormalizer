from template import Template
from templateNode import TemplateNode

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
