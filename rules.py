import re
from collections import defaultdict, Counter, deque

IP = re.compile(r"\d+\.\d+\.\d+\.\d+")
NUMBER = re.compile(r"^\d+$")
ISO_TS = re.compile(r"^\d{4}-\d{2}-\d{2}T")
SYSLOG_TS = re.compile(r"^[A-Z][a-z]{2}\s+\d{1,2}")
URL = re.compile(r"^(https?://)?([a-zA-Z0-9-]+\.)+[a-zA-Z]{2,}")
FILENAME = re.compile(r".+\.(exe|txt|docx|pptx|pdf|sh|bat|ps1)$")

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

USER_STATE = defaultdict(lambda: {"timestamps": deque(), "ips": Counter(), "processes": Counter()})
IP_STATE = defaultdict(lambda: {"users": Counter()})
TEMPLATE_TRANSITIONS = defaultdict(Counter)
LAST_TEMPLATE_PER_USER = {}

MONTHS = {m: i for i, m in enumerate(["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"],1)}

TEMPORAL_STATE = defaultdict(lambda: {"timestamps": deque(), "first_seen": None, "last_seen": None})
