#!/usr/bin/env python3
"""Strict source binding; workspace lives outside either source checkout."""
import argparse
import json
import re
import subprocess
from pathlib import Path

CORE_PLACEHOLDER = "6b9b7a75a924ba36b4e0c29c687cce041c00f84e"


def commit(value):
    if not re.fullmatch(r"[0-9a-f]{40}", value) or value in {"0" * 40, "f" * 40}:
        raise ValueError("invalid full commit")
    return value


def core_pin(path):
    data = Path(path).read_bytes()
    if not re.fullmatch(rb"[0-9a-f]{40}\n?", data):
        raise ValueError("invalid core pin")
    value = commit(data.decode("ascii").rstrip("\n"))
    if value == CORE_PLACEHOLDER:
        raise ValueError("core pin is an implementation placeholder; parent binding pending")
    return value


def check_checkout(path, expected, clean=True):
    expected = commit(expected)
    head = subprocess.check_output(["git", "-C", str(path), "rev-parse", "HEAD"], text=True).strip()
    if head != expected:
        raise ValueError("checkout commit mismatch")
    if clean and subprocess.check_output(["git", "-C", str(path), "status", "--porcelain", "--untracked-files=all"], text=True):
        raise ValueError("source checkout is not clean")
    return head


def workspace(core, kafka, output, integration=False):
    core, kafka, output = Path(core).resolve(), Path(kafka).resolve(), Path(output).resolve()
    for root in (core, kafka):
        if output.is_relative_to(root):
            raise ValueError("workspace must be outside source checkouts")
    roots = [core, kafka]
    if integration:
        roots.append(core / "integration_test")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("go 1.26.10\n\nuse (\n" + "".join(" " + json.dumps(str(p)) + "\n" for p in roots) + ")\n")
    return output


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    pin = sub.add_parser("pin")
    pin.add_argument("path")
    check = sub.add_parser("check")
    check.add_argument("path")
    check.add_argument("sha")
    work = sub.add_parser("workspace")
    work.add_argument("core")
    work.add_argument("kafka")
    work.add_argument("output")
    work.add_argument("--integration", action="store_true")
    args = parser.parse_args()
    if args.command == "pin":
        print(core_pin(args.path))
    elif args.command == "check":
        print(check_checkout(args.path, args.sha))
    else:
        print(workspace(args.core, args.kafka, args.output, args.integration))


if __name__ == "__main__":
    main()
