import io
import json
import tarfile
import tempfile
import unittest
from pathlib import Path

from verify_archive import CONFIG, CORE_URL, KAFKA_URL, LAYER, MANIFEST, digest, verify


def fixture_files(binary, layer_name="runner", environment=None, extra_labels=None):
    layer_buffer = io.BytesIO()
    with tarfile.open(fileobj=layer_buffer, mode="w") as layer:
        entry = tarfile.TarInfo(layer_name)
        entry.size, entry.mode = len(binary), 0o555
        layer.addfile(entry, io.BytesIO(binary))
    layer = layer_buffer.getvalue()
    labels = {
        "org.opencontainers.image.source": KAFKA_URL,
        "org.opencontainers.image.revision": "a" * 40,
        "org.opencontainers.image.version": "a" * 40,
        "io.trendyol.go-pq-cdc.core.revision": "b" * 40,
        "io.trendyol.go-pq-cdc.source-state": "committed",
    }
    labels.update(extra_labels or {})
    runtime = {"Entrypoint": ["/runner"], "User": "65532:65532", "Labels": labels}
    if environment is not None:
        runtime["Env"] = environment
    config = json.dumps({"os": "linux", "architecture": "amd64", "config": runtime, "rootfs": {"type": "layers", "diff_ids": [digest(layer)]}}).encode()
    config_descriptor = {"mediaType": CONFIG, "digest": digest(config), "size": len(config)}
    layer_descriptor = {"mediaType": LAYER, "digest": digest(layer), "size": len(layer)}
    manifest = json.dumps({"schemaVersion": 2, "config": config_descriptor, "layers": [layer_descriptor]}).encode()
    index = json.dumps({"schemaVersion": 2, "manifests": [{"mediaType": MANIFEST, "digest": digest(manifest), "size": len(manifest)}]}).encode()
    return {"oci-layout": b'{"imageLayoutVersion":"1.0.0"}', "index.json": index, "blobs/sha256/" + digest(manifest)[7:]: manifest, "blobs/sha256/" + digest(config)[7:]: config, "blobs/sha256/" + digest(layer)[7:]: layer}


def write_archive(path, files):
    with tarfile.open(path, "w") as archive:
        for name, data in files.items():
            entry = tarfile.TarInfo(name)
            entry.size = len(data)
            archive.addfile(entry, io.BytesIO(data))


class ArchiveTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.binary = b"\x7fELF\x02\x01" + b"\x00" * 12 + b"\x3e\x00" + b"test-binary"
        self.path = self.root / "runner"
        self.path.write_bytes(self.binary)
        self.archive = self.root / "image.tar"

    def check(self, files):
        write_archive(self.archive, files)
        return verify(self.archive, self.path, "a" * 40, "b" * 40)

    def test_valid_archive_binds_binary_platform_and_both_revisions(self):
        summary, binary = self.check(fixture_files(self.binary))
        self.assertEqual(self.binary, binary)
        self.assertEqual("b" * 40, summary["repositories"]["core"]["revision"])
        self.assertEqual(CORE_URL, summary["repositories"]["core"]["url"])
        self.assertEqual("a" * 40, summary["repositories"]["kafka"]["revision"])
        self.assertEqual(["linux/amd64"], summary["platforms"])
        self.assertNotEqual(summary["archiveSHA256"], summary["ociIndexDigest"])

    def test_only_constant_builder_path_and_numeric_provenance_are_allowed(self):
        self.check(fixture_files(self.binary, environment=["PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"], extra_labels={"io.buildah.version": "1.44.1"}))
        with self.assertRaises(ValueError):
            self.check(fixture_files(self.binary, environment=["CDC_PG_PASSWORD=private"]))
        with self.assertRaises(ValueError):
            self.check(fixture_files(self.binary, extra_labels={"runtime.password": "private"}))
        with self.assertRaises(ValueError):
            self.check(fixture_files(self.binary, extra_labels={"io.buildah.version": "private"}))

    def test_unreferenced_blob_cannot_smuggle_private_configuration(self):
        files = fixture_files(self.binary)
        secret = b"private runtime configuration must not be archived"
        files["blobs/sha256/" + digest(secret)[7:]] = secret
        with self.assertRaises(ValueError):
            self.check(files)

    def test_corrupt_descriptor_digest_and_size(self):
        files = fixture_files(self.binary)
        index = json.loads(files["index.json"])
        index["manifests"][0]["size"] += 1
        files["index.json"] = json.dumps(index).encode()
        with self.assertRaises(ValueError):
            self.check(files)
        files = fixture_files(self.binary)
        blob = next(name for name in files if name.startswith("blobs/"))
        files[blob] += b"corruption"
        with self.assertRaises(ValueError):
            self.check(files)

    def test_traversal_and_runtime_config_paths_are_never_extracted(self):
        for name in ["../../private", "runtime.yaml", ".git/config"]:
            with self.subTest(name=name), self.assertRaises(ValueError):
                self.check(fixture_files(self.binary, name))
        self.assertFalse((self.root.parent / "private").exists())

    def test_wrong_binary_and_wrong_core_revision_fail(self):
        self.path.write_bytes(b"wrong binary")
        with self.assertRaises(ValueError):
            self.check(fixture_files(self.binary))
        self.path.write_bytes(self.binary)
        write_archive(self.archive, fixture_files(self.binary))
        with self.assertRaises(ValueError):
            verify(self.archive, self.path, "a" * 40, "c" * 40)


if __name__ == "__main__":
    unittest.main()
