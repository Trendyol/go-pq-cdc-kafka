import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path

from build import command
from verify_tools import prepare_go, verify_buildx


class ToolsAndDriverTests(unittest.TestCase):
    def test_machine_output_does_not_merge_engine_diagnostics(self):
        diagnostics = io.StringIO()
        with redirect_stderr(diagnostics):
            output = command([sys.executable, "-c", "import sys;sys.stderr.write('platform warning\\n');print('{\"version\":\"verified\"}')"])
        self.assertEqual({"version": "verified"}, json.loads(output))
        self.assertIn("platform warning", diagnostics.getvalue())

    def test_buildx_corruption_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "buildx"
            path.write_bytes(b"wrong binary")
            with self.assertRaises(ValueError):
                verify_buildx(path, {"sha256": "0" * 64})

    def test_sdk_rejects_non_https_non_canonical_hosts_before_download(self):
        with tempfile.TemporaryDirectory() as directory:
            for url in ["file:///private", "http://dl.google.com/go/sdk", "https://unapproved.example/sdk"]:
                with self.subTest(url=url), self.assertRaises(ValueError):
                    prepare_go(Path(directory), {"url": url, "sha256": "0" * 64})


if __name__ == "__main__":
    unittest.main()
