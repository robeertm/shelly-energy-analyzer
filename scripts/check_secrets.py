#!/usr/bin/env python3
"""Scan the whole git history for hard-coded credentials (every added line of
every commit). Findings are printed masked, never in full. Known false
positives go into security-exceptions.json (kind "secret", fingerprint)."""
import hashlib
import json
import os
import re
import subprocess
import sys

RULES = {
    "github-token": re.compile(r"\bgh[pousr]_[A-Za-z0-9]{30,}\b|\bgithub_pat_[A-Za-z0-9_]{40,}\b"),
    "anthropic-key": re.compile(r"\bsk-ant-[A-Za-z0-9_\-]{20,}"),
    "openai-key": re.compile(r"\bsk-(?:proj-)?[A-Za-z0-9_\-]{40,}"),
    "slack-token": re.compile(r"\bxox[abprs]-[A-Za-z0-9\-]{10,}"),
    "aws-access-key": re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b"),
    "private-key": re.compile(r"-----BEGIN (?:RSA |EC |DSA |OPENSSH |PGP )?PRIVATE KEY"),
    "jwt": re.compile(r"\beyJ[A-Za-z0-9_\-]{10,}\.eyJ[A-Za-z0-9_\-]{10,}\.[A-Za-z0-9_\-]{10,}"),
    "telegram-bot-token": re.compile(r"\b\d{8,10}:AA[A-Za-z0-9_\-]{33}\b"),
}
FILE_RULES = re.compile(r"(^|/)(\.env|id_rsa|id_ed25519|[^/]+\.pem|[^/]+\.p12)$")
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def mask(s):
    return s[:4] + "…" + s[-2:]


def load_exceptions():
    path = os.path.join(ROOT, "security-exceptions.json")
    if not os.path.exists(path):
        return set()
    data = json.load(open(path, encoding="utf-8"))
    return {e["finding"] for e in data.get("exceptions", []) if e.get("kind") == "secret"}


def main():
    ok_prints = load_exceptions()
    out = subprocess.run(["git", "-C", ROOT, "log", "-p", "--unified=0", "--no-color", "--diff-filter=AM",
                          "--format=@@@ %H"], capture_output=True, text=True, errors="replace", check=True).stdout
    found = {}
    commit = path = ""
    for line in out.splitlines():
        if line.startswith("@@@ "):
            commit = line[4:]
        elif line.startswith("+++ b/"):
            path = line[6:]
            if FILE_RULES.search(path):
                found.setdefault(("file", path, ""), (commit, path))
        elif line.startswith("+") and not line.startswith("+++"):
            for name, rx in RULES.items():
                for m in rx.finditer(line):
                    fp = hashlib.sha256((name + m.group(0)).encode()).hexdigest()[:16]
                    found.setdefault((name, path, fp), (commit, mask(m.group(0))))
    bad = 0
    for (name, path, fp), (commit, shown) in sorted(found.items()):
        key = fp or hashlib.sha256((name + path).encode()).hexdigest()[:16]
        if key in ok_prints:
            continue
        bad += 1
        print("SECRET %s in %s (%s) first seen %s fingerprint %s" % (name, path, shown, commit[:8], key))
    print("secret scan: %d finding(s), %d unexcepted" % (len(found), bad))
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
