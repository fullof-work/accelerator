#!/usr/bin/env python3
"""Offline fixture and shell-boundary regressions; these are not real E2E."""

import hashlib
import io
import json
import os
from pathlib import Path
import platform
import shutil
import signal
import subprocess
import sys
import tarfile
import tempfile
import time
import unittest
import zlib

ROOT = Path(__file__).resolve().parents[2]
E2E = ROOT / "test/e2e"


def inspect_archive(path):
    with tarfile.open(path) as archive:
        members = archive.getmembers()
        assert len({member.name for member in members}) == len(members)
        assert all(member.isfile() and member.uid == member.gid == member.mtime == 0 for member in members)
        manifest, = json.load(archive.extractfile("manifest.json"))
        raw_config = archive.extractfile(manifest["Config"]).read()
        assert manifest["Config"] == hashlib.sha256(raw_config).hexdigest() + ".json"
        config = json.loads(raw_config)
        layers = [archive.extractfile(name).read() for name in manifest["Layers"]]
    assert len(layers) == 2
    assert config["rootfs"] == {
        "type": "layers",
        "diff_ids": ["sha256:" + hashlib.sha256(layer).hexdigest() for layer in layers],
    }
    files = {}
    for layer in layers:
        with tarfile.open(fileobj=io.BytesIO(layer)) as archive:
            for member in archive:
                assert not member.name.startswith("/") and ".." not in Path(member.name).parts
                assert member.mtime == 0
                if member.isfile():
                    files[member.name] = (member, archive.extractfile(member).read())
    return config, layers, files


class ManifestScriptsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Keep all generated files inside this checkout, including paths with
        # spaces to exercise scratch/tool forwarding and shell argument quoting.
        cls.temp = tempfile.TemporaryDirectory(prefix=".manifest regression-", dir=ROOT / "test/scripts")
        cls.base = Path(cls.temp.name)
        cls.fixtures = cls.base / "fixtures"
        subprocess.run([sys.executable, str(E2E / "lib/manifest_fixture.py"), str(cls.fixtures)], check=True)

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def setUp(self):
        self.case = Path(tempfile.mkdtemp(dir=self.base))
        self.tree = self.case / "assembled/test/e2e"
        (self.tree / "cases").mkdir(parents=True)
        (self.tree / "lib/accelerator").mkdir(parents=True)
        shutil.copyfile(E2E / "cases/image.manifest.sh", self.tree / "cases/image.manifest.sh")
        for name in ("manifest_fixture.py", "port_lease.sh"):
            shutil.copyfile(E2E / "lib" / name, self.tree / "lib/accelerator" / name)
        # This source regression exercises the accelerator case boundaries, not
        # the platform helper implementation. The real prepared workspace owns
        # common.sh; keep only the sourceable contract here.
        (self.tree / "lib/common.sh").write_text("# prepared platform common helper placeholder\n")
        self.bin = self.case / "bin"
        self.bin.mkdir()
        for tool in ("python3", "openssl", "unzip", "stat", "sha256sum", "awk", "grep", "sed", "dd",
                     "od", "tr", "truncate", "flock", "mktemp", "mkdir", "rm", "cat", "head", "sleep",
                     "dirname", "readlink", "env", "true"):
            source = shutil.which(tool)
            self.assertIsNotNone(source, f"regression prerequisite missing: {tool}")
            (self.bin / tool).symlink_to(source)
        self.scratch = self.case / "scratch area"
        self.scratch.mkdir()
        caller = self.case / "caller-home"
        (caller / ".docker").mkdir(parents=True)
        (caller / ".docker/config.json").write_text('{"credsStore":"must-not-be-used"}')
        self.env = {
            "PATH": str(self.bin), "BIN": str(self.bin), "TMPDIR": str(self.scratch),
            "E2E_LIB": str(self.tree / "lib"),
            "HOME": str(caller), "DOCKER_CONFIG": str(caller / ".docker"),
            "DOCKER_AUTH_CONFIG": "caller-secret-sentinel", "REGISTRY_PASSWORD": "caller-secret-sentinel",
        }
        self.control = {"store": "ready", "cached": False, "sudo": True}
        self.write_control()
        self.stub("manifest-ctl", "raise SystemExit('manifest must not run in boundary tests')")
        self.stub("sudo", '''
with (root / "sudo.log").open("a") as log:
    log.write(json.dumps(sys.argv[1:]) + "\\n")
assert sys.argv[1] == "-n"
assert "-E" not in sys.argv and "-nE" not in sys.argv
if not control["sudo"]:
    sys.exit(1)
os.execvp(sys.argv[2], sys.argv[2:])
''')
        self.stub("store-ctl", '''
with (root / "store.log").open("a") as log:
    log.write(sys.argv[1] + "\\n")
if sys.argv[1] == "init":
    sys.exit(0)
assert sys.argv[1] == "serve"
if control["store"] == "dead":
    print("intentional store startup failure", flush=True)
    sys.exit(29)
def stop(signum, frame):
    (root / "store.term").write_text(str(signum))
    if control["store"] != "stubborn":
        sys.exit(0)
signal.signal(signal.SIGTERM, stop)
if control["store"] == "slow":
    time.sleep(0.7)
if control["store"] in ("ready", "stubborn", "slow"):
    config = Path(sys.argv[sys.argv.index("--config") + 1]).read_text()
    port = int(config.splitlines()[0].rsplit(":", 1)[1])
    sock = socket.socket()
    sock.bind(("127.0.0.1", port))
    sock.listen()
(root / "store.pid").write_text(str(os.getpid()))
while True:
    time.sleep(0.05)
''')
        self.stub("flatten-ctl", '''
assert sys.argv[1] == "export", "boundary stub must never simulate numbered assertions"
output = Path(sys.argv[sys.argv.index("--output") + 1])
record = {
    "env": dict(os.environ), "args": sys.argv[1:],
    "input": hashlib.sha256(sys.stdin.buffer.read()).hexdigest(),
    "peer": hashlib.sha256((output.parent / "image-b.tar").read_bytes()).hexdigest(),
}
(root / "flatten.json").write_text(json.dumps(record))
sys.exit(73)  # Deliberately fail before any real EROFS/manifest assertion.
''')
        self.stub("docker", '''
with (root / "docker.log").open("a") as log:
    log.write(json.dumps({"args": sys.argv[1:], "env": dict(os.environ)}) + "\\n")
if sys.argv[1:3] == ["image", "inspect"]:
    sys.exit(0 if control["cached"] else 1)
if sys.argv[1] == "save" and control["cached"]:
    sys.stdout.buffer.write((root.parent / "fixtures/image-b.tar").read_bytes())
    sys.exit(0)
sys.exit("unexpected Docker operation (pull/tag/rm are forbidden)")
''')

    def write_control(self):
        (self.case / "control.json").write_text(json.dumps(self.control))

    def stub(self, name, body):
        path = self.bin / name
        path.write_text(f"#!{sys.executable}\n"
                        "import hashlib, json, os, signal, socket, sys, time\nfrom pathlib import Path\n"
                        f"root = Path({str(self.case)!r})\n"
                        "control = json.loads((root / 'control.json').read_text())\n" + body)
        path.chmod(0o755)

    def run_script(self, send_signal=None):
        with subprocess.Popen(["/bin/bash", str(self.tree / "cases/image.manifest.sh")], env=self.env,
                              stdout=subprocess.PIPE, stderr=subprocess.STDOUT, start_new_session=True) as proc:
            try:
                if send_signal is not None:
                    deadline = time.monotonic() + 5
                    while not (self.case / "store.pid").exists():
                        if proc.poll() is not None or time.monotonic() > deadline:
                            self.fail("store did not reach the signal-test boundary")
                        time.sleep(0.01)
                    proc.send_signal(send_signal)
                output = proc.communicate(timeout=10)[0].decode()
            finally:
                if proc.poll() is None:
                    os.killpg(proc.pid, signal.SIGKILL)
                    proc.communicate()
            self.assertEqual(list(self.scratch.iterdir()), [], output)
            pid_file = self.case / "store.pid"
            if pid_file.exists():
                pid = int(pid_file.read_text())
                try:
                    os.kill(pid, 0)
                except ProcessLookupError:
                    pass
                else:
                    os.kill(pid, signal.SIGKILL)
                    self.fail(f"owned store {pid} was not reaped: {output}")
            self.assertNotIn("Results:", output, "stubbed boundaries must not claim E2E success")
            return proc.returncode, output

    def test_archive_integrity_reproducibility_and_content(self):
        repeat = self.case / "repeat"
        subprocess.run([sys.executable, str(self.tree / "lib/accelerator/manifest_fixture.py"), str(repeat)], check=True)
        images = []
        architecture = {"x86_64": "amd64", "aarch64": "arm64"}[platform.machine()]
        for variant in ("a", "b"):
            path = self.fixtures / f"image-{variant}.tar"
            self.assertEqual(path.read_bytes(), (repeat / path.name).read_bytes())
            config, layers, files = inspect_archive(path)
            self.assertEqual((config["architecture"], config["os"]), (architecture, "linux"))
            self.assertEqual(config["config"]["User"], "1000:1000")
            self.assertEqual(config["config"]["WorkingDir"], "/fixture")
            self.assertEqual(config["config"]["Env"], ["MANIFEST_E2E=1"])
            self.assertEqual(len(config["history"]), 2)
            for name, size, mode in (("shared.bin", 4 * 1024 * 1024, 0o640),
                                     ("changed.bin", 1024 * 1024, 0o644)):
                member, data = files[f"fixture/{name}"]
                self.assertEqual((member.uid, member.gid, member.mode, member.size), (1000, 1000, mode, size))
                self.assertLess(data.count(b"\0"), size // 100)
                self.assertGreater(len(zlib.compress(data)), size * 0.99)
                self.assertEqual(len({data[i:i + 65536] for i in range(0, size, 65536)}), size // 65536)
            images.append((layers, files))
        self.assertEqual(images[0][0][0], images[1][0][0])
        self.assertNotEqual(images[0][0][1], images[1][0][1])
        self.assertEqual(images[0][1]["fixture/shared.bin"][1], images[1][1]["fixture/shared.bin"][1])
        self.assertNotEqual(images[0][1]["fixture/changed.bin"][1], images[1][1]["fixture/changed.bin"][1])

    def test_required_tools_and_binaries_fail_before_store(self):
        for tool in ("python3", "openssl", "unzip", "flock", "grep",
                     "flatten-ctl", "manifest-ctl", "store-ctl"):
            with self.subTest(tool=tool):
                path = self.bin / tool
                saved = self.case / "saved-tool"
                path.rename(saved)
                try:
                    code, output = self.run_script()
                    self.assertEqual(code, 1, output)
                    self.assertIn(tool, output)
                    self.assertIn("required", output)
                    self.assertFalse((self.case / "store.log").exists())
                finally:
                    saved.rename(path)

    def test_missing_fixture_or_port_helper_fails_before_store(self):
        for helper in ("manifest_fixture.py", "port_lease.sh"):
            with self.subTest(helper=helper):
                path = self.tree / "lib/accelerator" / helper
                path.unlink()
                try:
                    code, output = self.run_script()
                    self.assertEqual(code, 1, output)
                    self.assertIn("required prepared accelerator helper missing:", output)
                    self.assertIn(helper, output)
                    self.assertFalse((self.case / "store.log").exists())
                finally:
                    shutil.copyfile(E2E / "lib" / helper, path)

    def test_retired_erofs_selector_does_not_block_the_linked_generator(self):
        self.env["MKFS_EROFS_PATH"] = str(self.case / "missing-mkfs")
        self.assertFalse((self.bin / "mkfs.erofs").exists())
        code, output = self.run_script()
        self.assertEqual(code, 73, output)  # The intended export-error sentinel.
        record = json.loads((self.case / "flatten.json").read_text())
        self.assertNotIn("MKFS_EROFS_PATH", record["env"])
        self.assertTrue((self.case / "store.term").exists())

    def test_missing_generated_archive_fails_before_store(self):
        (self.tree / "lib/accelerator/manifest_fixture.py").write_text("# Deliberately produce no archives.\n")
        code, output = self.run_script()
        self.assertEqual(code, 1, output)
        self.assertIn("required image archive missing or empty", output)
        self.assertFalse((self.case / "store.log").exists())

    @unittest.skipIf(os.geteuid() == 0, "root does not need sudo")
    def test_noninteractive_sudo_is_required_before_store(self):
        self.control["sudo"] = False
        self.write_control()
        code, output = self.run_script()
        self.assertEqual(code, 1, output)
        self.assertIn("noninteractive sudo", output)
        self.assertFalse((self.case / "store.log").exists())

    def test_explicit_cached_image_miss_is_offline_and_isolated(self):
        for variable in ("IMAGE_A", "IMAGE_B"):
            with self.subTest(variable=variable):
                self.env[variable] = "missing:local"
                code, output = self.run_script()
                self.assertEqual(code, 1, output)
                self.assertIn(f"{variable}=missing:local is not cached", output)
                self.assertFalse((self.case / "store.log").exists())
                call, = [json.loads(line) for line in (self.case / "docker.log").read_text().splitlines()]
                self.assertEqual(call["args"], ["image", "inspect", "missing:local"])
                self.assert_isolated(call["env"])
                (self.case / "docker.log").unlink()
                del self.env[variable]

    def assert_isolated(self, environment):
        self.assertTrue(Path(environment["HOME"]).is_relative_to(self.scratch))
        self.assertTrue(Path(environment["DOCKER_CONFIG"]).is_relative_to(self.scratch))
        self.assertNotIn("DOCKER_AUTH_CONFIG", environment)
        self.assertNotIn("REGISTRY_PASSWORD", environment)

    def test_single_cached_override_preserves_the_other_default(self):
        self.control["cached"] = True
        self.write_control()
        for variable in ("IMAGE_A", "IMAGE_B"):
            with self.subTest(variable=variable):
                self.env[variable] = "cached:local"
                code, output = self.run_script()
                self.assertEqual(code, 73, output)
                calls = [json.loads(line) for line in (self.case / "docker.log").read_text().splitlines()]
                self.assertEqual([call["args"] for call in calls],
                                 [["image", "inspect", "cached:local"], ["save", "cached:local"]])
                for call in calls:
                    self.assert_isolated(call["env"])
                record = json.loads((self.case / "flatten.json").read_text())
                variant = "b" if variable == "IMAGE_A" else "a"
                self.assertEqual(record["input"], hashlib.sha256((self.fixtures / f"image-{variant}.tar").read_bytes()).hexdigest())
                self.assertEqual(record["peer"], hashlib.sha256((self.fixtures / "image-b.tar").read_bytes()).hexdigest())
                (self.case / "docker.log").unlink()
                (self.case / "store.pid").unlink()
                del self.env[variable]

    def test_default_needs_no_docker_and_preserves_export_error(self):
        (self.bin / "docker").unlink()
        code, output = self.run_script()
        self.assertEqual(code, 73, output)
        self.assertTrue((self.case / "store.term").exists())
        record = json.loads((self.case / "flatten.json").read_text())
        self.assertEqual(record["input"], hashlib.sha256((self.fixtures / "image-a.tar").read_bytes()).hexdigest())
        self.assert_isolated(record["env"])
        self.assertNotIn("MKFS_EROFS_PATH", record["env"])
        self.assertTrue(Path(record["env"]["TMPDIR"]).is_relative_to(self.scratch))

    def test_readiness_does_not_impose_the_old_nonfatal_probe_deadline(self):
        self.control["store"] = "slow"
        self.write_control()
        code, output = self.run_script()
        self.assertEqual(code, 73, output)
        self.assertNotIn("did not become ready", output)
        self.assertTrue((self.case / "store.term").exists())

    def test_store_never_ready_or_early_exit_fails_with_diagnostics(self):
        for mode in ("never", "dead"):
            with self.subTest(mode=mode):
                self.control["store"] = mode
                self.write_control()
                code, output = self.run_script()
                self.assertEqual(code, 1, output)
                self.assertIn("did not become ready", output)
                self.assertNotIn("  store-ctl listen=", output)
                self.assertFalse((self.case / "flatten.json").exists())
                if mode == "dead":
                    self.assertIn("intentional store startup failure", output)
                (self.case / "store.pid").unlink(missing_ok=True)

    def test_signal_status_and_owned_store_cleanup(self):
        self.control["store"] = "never"
        self.write_control()
        for sig, status in ((signal.SIGINT, 130), (signal.SIGTERM, 143)):
            with self.subTest(signal=sig):
                code, output = self.run_script(send_signal=sig)
                self.assertEqual(code, status, output)
                self.assertTrue((self.case / "store.term").exists())
                (self.case / "store.pid").unlink()

    def test_stubborn_owned_store_is_killed_without_touching_other_processes(self):
        self.control["store"] = "stubborn"
        self.write_control()
        with subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"]) as unrelated:
            try:
                code, output = self.run_script()
                self.assertEqual(code, 73, output)
                self.assertTrue((self.case / "store.term").exists())
                self.assertIsNone(unrelated.poll())
            finally:
                unrelated.terminate()
                unrelated.wait(timeout=3)

    def test_startup_boundary_normal_exit_cleanup(self):
        # Execute the real startup/EXIT trap, stopping before the numbered
        # assertions. No stub supplies a successful flatten or manifest result.
        script = self.tree / "cases/image.manifest.sh"
        marker = '# ============================================================\necho ""\necho "=== Test 1: Flatten'
        startup, _ = script.read_text().split(marker, 1)
        script.write_text(startup + "\nexit 0\n")
        code, output = self.run_script()
        self.assertEqual(code, 0, output)
        self.assertTrue((self.case / "store.term").exists())
        self.assertFalse((self.case / "flatten.json").exists())


if __name__ == "__main__":
    unittest.main(verbosity=2)
