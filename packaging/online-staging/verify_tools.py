#!/usr/bin/env python3
"""Verify the separately downloaded Linux amd64 Go SDK and Buildx binary."""
import argparse
import hashlib
import http.client
import json
import re
import tarfile
from pathlib import Path
from urllib.parse import urlsplit


def prepare_go(output, pin):
    output = Path(output).resolve()
    output.mkdir(parents=True, exist_ok=True)
    archive_path = output / "go.tar.gz"
    parsed = urlsplit(pin["url"])
    if parsed.scheme != "https" or parsed.hostname != "dl.google.com" or parsed.netloc != "dl.google.com" or parsed.query or parsed.fragment:
        raise ValueError("unsupported Go SDK download URL")
    connection = http.client.HTTPSConnection(parsed.hostname, timeout=60)
    try:
        connection.request("GET", parsed.path)
        response = connection.getresponse()
        if response.status != 200:
            raise ValueError("Go SDK download failed")
        data = response.read((80 << 20) + 1)
    finally:
        connection.close()
    if len(data) > 80 << 20 or hashlib.sha256(data).hexdigest() != pin["sha256"]:
        raise ValueError("Go SDK integrity check failed")
    archive_path.write_bytes(data)
    with tarfile.open(archive_path, "r:gz") as archive:
        for member in archive.getmembers():
            path = (output / member.name).resolve()
            if not member.name.startswith("go/") or not path.is_relative_to(output) or not (member.isfile() or member.isdir()):
                raise ValueError("unsafe Go SDK archive")
        archive.extractall(output, filter="data")
    return output / "go"


def verify_buildx(binary, pin):
    if hashlib.sha256(Path(binary).read_bytes()).hexdigest() != pin["sha256"]:
        raise ValueError("Buildx binary integrity check failed")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--prepare-go")
    parser.add_argument("--verify-buildx")
    parser.add_argument("--buildkit-image", action="store_true")
    args = parser.parse_args()
    pins = json.loads(Path(__file__).with_name("tools.json").read_text())
    if args.prepare_go:
        print(prepare_go(args.prepare_go, pins["go"]))
    if args.verify_buildx:
        verify_buildx(args.verify_buildx, pins["buildx"])
        print("Buildx binary checksum verified")
    if args.buildkit_image:
        image = pins["buildkitImage"]
        if not re.fullmatch(r"moby/buildkit@sha256:[0-9a-f]{64}", image):
            raise ValueError("invalid immutable BuildKit image")
        print(image)
    if not args.prepare_go and not args.verify_buildx and not args.buildkit_image:
        parser.error("a verification operation is required")


if __name__ == "__main__":
    main()
