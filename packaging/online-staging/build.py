#!/usr/bin/env python3
"""Build a commit-bound scratch OCI archive; never connect to a source database."""
import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import uuid
from pathlib import Path

from source_binding import check_checkout, commit, workspace
from verify_archive import verify


def command(args, env=None, cwd=None, timeout=300):
    completed = subprocess.run(args, env=env, cwd=cwd, text=True, capture_output=True, check=True, timeout=timeout)
    if completed.stderr:
        print(completed.stderr, file=sys.stderr, end="")
    return completed.stdout.strip()


def manifests(core, kafka):
    return {str(root / name): hashlib.sha256((root / name).read_bytes()).hexdigest() for root in (core, kafka) for name in ("go.mod", "go.sum")}


def builder_info(args, prefix):
    if args.engine == "podman":
        return {"driver": "podman-local", "version": command(prefix + ["version", "--format", "{{.Client.Version}}/{{.Server.Version}}"])}
    if not re.fullmatch(r"[a-zA-Z0-9_-]+", args.builder):
        raise ValueError("invalid owned builder name")
    pins = json.loads(Path(__file__).with_name("tools.json").read_text())
    nodes = json.loads(Path(args.builder_nodes).read_text())
    if len(nodes) != 1 or not re.fullmatch(r"[a-zA-Z0-9_-]+", nodes[0]["name"]):
        raise ValueError("expected one owned builder node")
    name = "buildx_buildkit_" + nodes[0]["name"]
    image_id = command(["docker", "inspect", name, "--format", "{{.Image}}"])
    repo_digests = json.loads(command(["docker", "image", "inspect", image_id, "--format", "{{json .RepoDigests}}"] ))
    if not any(value.endswith(pins["buildkitImage"].split("@", 1)[1]) for value in repo_digests):
        raise ValueError("BuildKit image digest mismatch")
    return {"driver": "docker-container", "name": args.builder, "buildxVersion": command(["docker", "buildx", "version"]), "buildkitVersion": nodes[0]["buildkit"], "image": pins["buildkitImage"], "imageID": image_id, "repoDigests": repo_digests}


def package_image(args, context, archive, info):
    prefix = ["docker", "buildx"]
    if args.engine == "podman":
        prefix = ["podman"]
        if args.podman_connection:
            prefix += ["--connection", args.podman_connection]
    binding = ["--build-arg", "KAFKA_REVISION=" + args.kafka_sha, "--build-arg", "CORE_REVISION=" + args.core_sha, "--build-arg", "SOURCE_STATE=" + info["sourceState"]]
    image = "localhost/online-staging-oci-" + uuid.uuid4().hex
    try:
        if args.engine == "podman":
            command(prefix + ["build", "--network=none", "--platform", args.platform, "--format", "oci", "--tag", image] + binding + [str(context)], timeout=600)
            command(prefix + ["save", "--format", "oci-archive", "--output", str(archive), image])
            version = json.loads(command(prefix + ["run", "--rm", "--network=none", "--read-only", "--cap-drop=all", "--label", "cdc.test.owner=online-staging-oci", image, "--version"], timeout=30))
        else:
            command(prefix + ["build", "--builder", args.builder, "--network=none", "--platform", args.platform, "--provenance=false", "--sbom=false", "--output", "type=oci,dest=" + str(archive)] + binding + [str(context)], timeout=600)
            version = None
        info["builder"] = builder_info(args, prefix)
        return version
    finally:
        if args.engine == "podman":
            subprocess.run(prefix + ["image", "rm", image], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=30)


def build(args):
    args.kafka_sha, args.core_sha = commit(args.kafka_sha), commit(args.core_sha)
    core, kafka, output = Path(args.core).resolve(), Path(args.kafka).resolve(), Path(args.output).resolve()
    if output.is_relative_to(core) or output.is_relative_to(kafka):
        raise ValueError("build output must be outside source checkouts")
    check_checkout(core, args.core_sha, not args.local_uncommitted)
    check_checkout(kafka, args.kafka_sha, not args.local_uncommitted)
    before = manifests(core, kafka)
    output.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="online-oci-", dir=output) as temp:
        temp = Path(temp)
        context = temp / "context"
        context.mkdir()
        work = workspace(core, kafka, temp / "go.work")
        env = dict(os.environ, GOTOOLCHAIN="go1.26.10", GOWORK=str(work), GOFLAGS="-mod=readonly", CGO_ENABLED="0", GOOS="linux", GOARCH=args.platform.split("/")[1])
        resolved = json.loads(command(["go", "list", "-m", "-json", "github.com/Trendyol/go-pq-cdc"], env, kafka))
        if Path(resolved["Dir"]).resolve() != core:
            raise ValueError("workspace resolved a published core instead of the checkout")
        go_version = command(["go", "version"], env, kafka)
        if not go_version.startswith("go version go1.26.10 "):
            raise ValueError("wrong build toolchain")
        state = "uncommitted-local" if args.local_uncommitted else "committed"
        flags = "-s -w -X main.kafkaRevision=" + args.kafka_sha + " -X main.coreRevision=" + args.core_sha + " -X main.sourceState=" + state
        command(["go", "build", "-trimpath", "-ldflags", flags, "-o", str(context / "runner"), "./cmd/online-staging"], env, kafka)
        shutil.copyfile(Path(__file__).with_name("Dockerfile"), context / "Dockerfile")
        if {p.name for p in context.iterdir()} != {"runner", "Dockerfile"}:
            raise ValueError("unexpected packaging context")
        archive = temp / "runner.oci.tar"
        info = {"sourceState": state, "goVersion": go_version, "runner": {"imageOS": args.runner_image_os, "imageVersion": args.runner_image_version}, "workflow": {"runID": args.run_id, "repository": "Trendyol/go-pq-cdc-kafka"}}
        version = package_image(args, context, archive, info)
        manifest, runner = verify(archive, context / "runner", args.kafka_sha, args.core_sha, args.platform, state)
        if args.engine == "buildx":
            extracted = temp / "runner"
            extracted.write_bytes(runner)
            extracted.chmod(0o555)
            version = json.loads(command([str(extracted), "--version"], timeout=30))
        expected = {"kafkaRevision": args.kafka_sha, "coreRevision": args.core_sha, "sourceState": state, "goVersion": "go1.26.10", "platform": args.platform}
        if version != expected:
            raise ValueError("extracted/image runner version mismatch")
        if before != manifests(core, kafka):
            raise ValueError("source module manifests changed during build")
        check_checkout(core, args.core_sha, not args.local_uncommitted)
        check_checkout(kafka, args.kafka_sha, not args.local_uncommitted)
        manifest.update(info)
        archive_name = "online-staging-" + args.kafka_sha + "-" + args.platform.replace("/", "-") + ".oci.tar"
        shutil.move(archive, output / archive_name)
        (output / "build-manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
        (output / "version.json").write_text(json.dumps(version, indent=2, sort_keys=True) + "\n")
        names = [archive_name, "build-manifest.json", "version.json"]
        (output / "SHA256SUMS").write_text("".join(hashlib.sha256((output / name).read_bytes()).hexdigest() + "  " + name + "\n" for name in names))
        print(json.dumps({"archive": archive_name, "sourceState": state, "archiveSHA256": manifest["archiveSHA256"]}))


def main():
    parser = argparse.ArgumentParser()
    for name in ("core", "kafka", "core-sha", "kafka-sha", "output"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--engine", choices=("podman", "buildx"), required=True)
    parser.add_argument("--platform", choices=("linux/amd64",), default="linux/amd64")
    parser.add_argument("--podman-connection", default="")
    parser.add_argument("--builder", default="")
    parser.add_argument("--builder-nodes", default="")
    parser.add_argument("--run-id", default="local")
    parser.add_argument("--runner-image-os", default="local")
    parser.add_argument("--runner-image-version", default="local")
    parser.add_argument("--local-uncommitted", action="store_true", help="local qualification only, explicitly not a committed source binding")
    build(parser.parse_args())


if __name__ == "__main__":
    main()
