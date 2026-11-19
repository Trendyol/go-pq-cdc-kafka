#!/usr/bin/env python3
"""Validate OCI descriptors and the allowlisted scratch filesystem before upload."""
import argparse
import gzip
import hashlib
import io
import json
import re
import tarfile
from pathlib import Path

from source_binding import commit

LIMIT = 128 << 20
MANIFEST = "application/vnd.oci.image.manifest.v1+json"
INDEX = "application/vnd.oci.image.index.v1+json"
CONFIG = "application/vnd.oci.image.config.v1+json"
LAYER = "application/vnd.oci.image.layer.v1.tar"
KAFKA_URL = "https://github.com/Trendyol/go-pq-cdc-kafka"
CORE_URL = "https://github.com/Trendyol/go-pq-cdc"


def digest(data):
    return "sha256:" + hashlib.sha256(data).hexdigest()


def regular_files(archive, outer=False):
    files = {}
    members = archive.getmembers()
    if len(members) > 32:
        raise ValueError("unexpected archive entry count")
    for member in members:
        name = member.name.removeprefix("./")
        if member.isdir() and name in {".", "blobs", "blobs/sha256"}:
            continue
        if not member.isfile() or member.size > LIMIT or name in files:
            raise ValueError("unsafe archive entry")
        allowed = name == "runner"
        if outer:
            allowed = name in {"index.json", "oci-layout"} or re.fullmatch(r"blobs/sha256/[0-9a-f]{64}", name)
        if not allowed:
            raise ValueError("archive contains a non-allowlisted path")
        stream = archive.extractfile(member)
        if stream is None:
            raise ValueError("missing archive entry")
        with stream:
            files[name] = stream.read(LIMIT + 1)
        if len(files[name]) != member.size:
            raise ValueError("archive entry size mismatch")
        if not outer and member.mode & 0o111 == 0:
            raise ValueError("runner is not executable")
    return files


def verify(archive_path, binary_path, kafka_sha, core_sha, platform="linux/amd64", source_state="committed"):
    commit(kafka_sha)
    commit(core_sha)
    if platform not in {"linux/amd64", "linux/arm64"} or source_state not in {"committed", "uncommitted-local"}:
        raise ValueError("unsupported build binding")
    archive_path, binary_path = Path(archive_path), Path(binary_path)
    if archive_path.stat().st_size > 2 * LIMIT:
        raise ValueError("oversized archive")
    with tarfile.open(archive_path, "r:*") as archive:
        files = regular_files(archive, outer=True)
    if json.loads(files["oci-layout"]) != {"imageLayoutVersion": "1.0.0"}:
        raise ValueError("unsupported OCI layout")
    index_data = files["index.json"]
    index = json.loads(index_data)
    referenced = {"index.json", "oci-layout"}

    def descriptor(item):
        value = item["digest"]
        if not re.fullmatch(r"sha256:[0-9a-f]{64}", value):
            raise ValueError("invalid descriptor digest")
        name = "blobs/sha256/" + value.split(":")[1]
        referenced.add(name)
        data = files[name]
        if item["size"] != len(data) or digest(data) != value:
            raise ValueError("descriptor size/digest mismatch")
        return data

    if index.get("schemaVersion") != 2 or len(index.get("manifests", [])) != 1:
        raise ValueError("expected one OCI image")
    image_descriptor = index["manifests"][0]
    if image_descriptor["mediaType"] == INDEX:
        nested = json.loads(descriptor(image_descriptor))
        if nested.get("schemaVersion") != 2 or len(nested.get("manifests", [])) != 1:
            raise ValueError("unexpected nested image index")
        image_descriptor = nested["manifests"][0]
    if image_descriptor["mediaType"] != MANIFEST:
        raise ValueError("unsupported image manifest")
    image = json.loads(descriptor(image_descriptor))
    if image.get("schemaVersion") != 2 or image["config"]["mediaType"] != CONFIG or len(image.get("layers", [])) != 1:
        raise ValueError("expected one scratch runner layer")
    config = json.loads(descriptor(image["config"]))
    operating_system, architecture = platform.split("/")
    if config.get("os") != operating_system or config.get("architecture") != architecture:
        raise ValueError("image platform mismatch")
    if "platform" in image_descriptor:
        declared = image_descriptor["platform"]
        if declared.get("os") != operating_system or declared.get("architecture") != architecture:
            raise ValueError("descriptor platform mismatch")
    runtime = config["config"]
    expected_labels = {
        "org.opencontainers.image.source": KAFKA_URL,
        "org.opencontainers.image.revision": kafka_sha,
        "org.opencontainers.image.version": kafka_sha,
        "io.trendyol.go-pq-cdc.core.revision": core_sha,
        "io.trendyol.go-pq-cdc.source-state": source_state,
    }
    labels = dict(runtime.get("Labels", {}))
    buildah_version = labels.pop("io.buildah.version", None)
    if buildah_version is not None and not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", buildah_version):
        raise ValueError("invalid builder provenance label")
    if labels != expected_labels or runtime.get("Entrypoint") != ["/runner"] or runtime.get("User") != "65532:65532":
        raise ValueError("image labels/entrypoint/user mismatch")
    # Buildah/BuildKit may supply this constant PATH for a scratch image. No
    # credentials, runtime configuration or arbitrary inherited ENV is allowed.
    default_path = ["PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"]
    if runtime.get("Env") not in (None, [], default_path) or runtime.get("Cmd"):
        raise ValueError("unexpected baked runtime environment/command")
    layer_descriptor = image["layers"][0]
    layer = descriptor(layer_descriptor)
    if layer_descriptor["mediaType"] == LAYER + "+gzip":
        with gzip.GzipFile(fileobj=io.BytesIO(layer)) as compressed:
            layer = compressed.read(LIMIT + 1)
    elif layer_descriptor["mediaType"] != LAYER:
        raise ValueError("unsupported layer compression")
    if len(layer) > LIMIT or config["rootfs"] != {"type": "layers", "diff_ids": [digest(layer)]}:
        raise ValueError("root filesystem digest mismatch")
    with tarfile.open(fileobj=io.BytesIO(layer), mode="r:") as contents:
        runtime_files = regular_files(contents)
    if set(runtime_files) != {"runner"}:
        raise ValueError("image must contain only /runner")
    runner = runtime_files["runner"]
    expected_binary = binary_path.read_bytes()
    machine = 62 if architecture == "amd64" else 183
    if runner != expected_binary or runner[:6] != b"\x7fELF\x02\x01" or int.from_bytes(runner[18:20], "little") != machine:
        raise ValueError("runner checksum/ELF architecture mismatch")
    if set(files) != referenced:
        raise ValueError("unexpected unreferenced archive blobs")
    summary = {
        "schemaVersion": 1,
        "repositories": {"core": {"url": CORE_URL, "revision": core_sha}, "kafka": {"url": KAFKA_URL, "revision": kafka_sha}},
        "verifiedCorePin": core_sha,
        "sourceState": source_state,
        "platforms": [platform],
        "binarySHA256": {platform: digest(runner)},
        "ociIndexDigest": digest(index_data),
        "ociManifestDigests": [image_descriptor["digest"]],
        "ociConfigDigest": image["config"]["digest"],
        "archiveSHA256": digest(archive_path.read_bytes()),
        "entrypoint": ["/runner"],
        "user": "65532:65532",
    }
    return summary, runner


def main():
    parser = argparse.ArgumentParser()
    for name in ("archive", "binary", "kafka-sha", "core-sha", "manifest"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--platform", default="linux/amd64")
    parser.add_argument("--source-state", default="committed")
    parser.add_argument("--extract")
    args = parser.parse_args()
    summary, runner = verify(args.archive, args.binary, args.kafka_sha, args.core_sha, args.platform, args.source_state)
    Path(args.manifest).write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    if args.extract:
        Path(args.extract).write_bytes(runner)
        Path(args.extract).chmod(0o555)


if __name__ == "__main__":
    main()
