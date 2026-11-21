#!/usr/bin/env python3
"""Generate or check LICENSE-THIRD-PARTY with the shared licence generator.

The generator is one file, kept in github.com/lightwebinc/ci so that every
repository writes its LICENSE-THIRD-PARTY the same way. This file only fetches
it at the commit pinned below, checks its SHA-256, keeps it under
~/.cache/lightwebinc-ci/ and runs it with the arguments given here.

Usage:  gen-third-party-licenses.py <repo-dir> [--check]

To move to a newer generator, change REF and SHA256 together and regenerate.
LICENCES_GEN=<path> runs a local copy instead, without fetching.
"""
import hashlib
import os
import sys
import tempfile
import urllib.request

REF = "addd14064ef61ebf08d01de9b80bda61438880fb"  # lightwebinc/ci v0.7.1
SHA256 = "ac1c1fcd957cda60dd446abab17a0c7f8e661c7cba3ea4771770c9f3e5f7510d"
URL = f"https://raw.githubusercontent.com/lightwebinc/ci/{REF}/scripts/gen-third-party-licenses.py"


def verified(path):
    try:
        with open(path, "rb") as f:
            return hashlib.sha256(f.read()).hexdigest() == SHA256
    except OSError:
        return False


def generator():
    local = os.environ.get("LICENCES_GEN")
    if local:
        return local
    cache = os.path.join(
        os.environ.get("XDG_CACHE_HOME") or os.path.join(os.path.expanduser("~"), ".cache"),
        "lightwebinc-ci", REF,
    )
    path = os.path.join(cache, "gen-third-party-licenses.py")
    if verified(path):
        return path
    os.makedirs(cache, exist_ok=True)
    try:
        with urllib.request.urlopen(URL, timeout=60) as r:
            data = r.read()
    except OSError as e:
        sys.exit(f"cannot fetch the licence generator from {URL}: {e}")
    if hashlib.sha256(data).hexdigest() != SHA256:
        sys.exit(f"the licence generator at {URL} does not match its pinned SHA-256")
    # Written beside its final name and renamed, so a concurrent run never
    # executes a half-written file.
    fd, tmp = tempfile.mkstemp(dir=cache)
    with os.fdopen(fd, "wb") as f:
        f.write(data)
    os.replace(tmp, path)
    return path


if __name__ == "__main__":
    gen = generator()
    os.execv(sys.executable, [sys.executable, gen] + sys.argv[1:])
