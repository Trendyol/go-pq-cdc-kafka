import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from source_binding import CORE_PLACEHOLDER, check_checkout, core_pin, workspace


class SourceBindingTests(unittest.TestCase):
    def test_pin_valid_missing_malformed_multiple_and_placeholder(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "pin"
            with self.assertRaises(FileNotFoundError):
                core_pin(path)
            path.write_text("a" * 40 + "\n")
            self.assertEqual("a" * 40, core_pin(path))
            for value in ["", "main\n", "a" * 39, "a" * 40 + "\n" + "b" * 40, CORE_PLACEHOLDER, "0" * 40, "f" * 40]:
                path.write_text(value)
                with self.assertRaises(ValueError):
                    core_pin(path)

    def test_checkout_mismatch_and_dirty_checkout_fail(self):
        with patch("source_binding.subprocess.check_output", side_effect=["a" * 40 + "\n", ""]):
            self.assertEqual("a" * 40, check_checkout("checkout", "a" * 40))
        with patch("source_binding.subprocess.check_output", return_value="b" * 40), self.assertRaises(ValueError):
            check_checkout("checkout", "a" * 40)
        with patch("source_binding.subprocess.check_output", side_effect=["a" * 40, " M go.mod"]), self.assertRaises(ValueError):
            check_checkout("checkout", "a" * 40)

    def test_workspace_is_external_and_can_include_integration(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            core, kafka = root / "core", root / "kafka"
            output = workspace(core, kafka, root / "temporary" / "go.work", integration=True)
            self.assertIn("go 1.26.10", output.read_text())
            self.assertIn(str(core / "integration_test"), output.read_text())
            with self.assertRaises(ValueError):
                workspace(core, kafka, kafka / "go.work")


if __name__ == "__main__":
    unittest.main()
