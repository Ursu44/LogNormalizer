from rules import *

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