"""Exercise scratch-disk safety decisions without touching real block devices."""

import os
from pathlib import Path
import subprocess
import tempfile
import unittest


MOCK_COMMAND = r"""#!/bin/bash
set -e
case "$(basename "$0")" in
  mountpoint) test -e "$TEST_ROOT/mounted" ;;
  findmnt) echo 259:1 ;;
  lsblk)
    if [[ "$*" == *MAJ:MIN* ]]; then echo 259:1
    elif [[ -e "$TEST_ROOT/partitioned" ]]; then printf 'disk\npart\n'
    else echo disk; fi ;;
  wipefs)
    if [[ -e "$TEST_ROOT/badfs" ]]; then echo xfs
    elif [[ -e "$TEST_ROOT/fs" ]]; then echo ext4; fi ;;
  blkid) if [[ "$*" == *TYPE* ]]; then echo ext4; else echo gsql-scratch; fi ;;
  mkfs.ext4) echo format >> "$TEST_ROOT/actions"; touch "$TEST_ROOT/fs" ;;
  mount) echo mount >> "$TEST_ROOT/actions"; touch "$TEST_ROOT/mounted" ;;
  install) : ;;
  *) exit 99 ;;
esac
"""


class ScratchSetupTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="gsql-scratch-test-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        bindir = self.root / "bin"
        bindir.mkdir()
        for name in (
            "mountpoint",
            "findmnt",
            "lsblk",
            "wipefs",
            "blkid",
            "mkfs.ext4",
            "mount",
            "install",
        ):
            command = bindir / name
            command.write_text(MOCK_COMMAND)
            command.chmod(0o755)
        self.model = self.root / "sys/block/nvme1n1/device/model"
        self.model.parent.mkdir(parents=True)
        self.model.write_text("Amazon EC2 NVMe Instance Storage\n")
        source = (Path(__file__).parent / "scratch-setup.sh").read_text()
        self.script = self.root / "setup.sh"
        self.script.write_text(
            source.replace("/sys/block/", str(self.root) + "/sys/block/")
            .replace("/dev/", str(self.root) + "/dev/")
            .replace("/scratch", str(self.root) + "/scratch")
        )
        self.env = dict(
            os.environ, TEST_ROOT=str(self.root), PATH=str(bindir) + ":" + os.environ["PATH"]
        )

    def run_setup(self):
        return subprocess.run(
            ["bash", str(self.script)], env=self.env, capture_output=True, text=True
        )

    def test_fresh_disk_and_reboot(self):
        first = self.run_setup()
        self.assertEqual(first.returncode, 0, first.stderr)
        (self.root / "mounted").unlink()
        reboot = self.run_setup()
        self.assertEqual(reboot.returncode, 0, reboot.stderr)
        self.assertEqual(
            (self.root / "actions").read_text().splitlines(), ["format", "mount", "mount"]
        )

    def test_refuses_unexpected_disk_layout(self):
        for marker in ("badfs", "partitioned"):
            with self.subTest(marker=marker):
                (self.root / marker).touch()
                result = self.run_setup()
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("refusing to format", result.stderr)
                self.assertFalse((self.root / "actions").exists())
                (self.root / marker).unlink()

    def test_refuses_ebs_or_multiple_instance_store_disks(self):
        self.model.write_text("Amazon Elastic Block Store\n")
        self.assertNotEqual(self.run_setup().returncode, 0)
        self.model.write_text("Amazon EC2 NVMe Instance Storage\n")
        other = self.root / "sys/block/nvme2n1/device/model"
        other.parent.mkdir(parents=True)
        other.write_text(self.model.read_text())
        self.assertNotEqual(self.run_setup().returncode, 0)
        self.assertFalse((self.root / "actions").exists())


if __name__ == "__main__":
    unittest.main()
