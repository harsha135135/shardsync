"""Shared helpers for the ShardSync demonstration and benchmarks (standard library only).

Everything here drives the real release binary in separate processes: it builds
`shardsync` without test features, generates a deterministic dataset, runs a receiver and a
sender, interrupts them with SIGKILL, and reads the receiver's status and diagnostics over
HTTPS. Measurements are taken by the operating system (`/usr/bin/time -v`, `/proc`) or by
the receiver's own counters, never estimated.
"""

from __future__ import annotations

import hashlib
import json
import os
import platform
import random
import shutil
import signal
import ssl
import subprocess
import threading
import time
import urllib.request
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
MIB = 1024 * 1024


def run(args: list[str], **kwargs) -> subprocess.CompletedProcess:
    return subprocess.run(args, check=True, text=True, capture_output=True, **kwargs)


def build_release() -> Path:
    """Builds the release binary with no optional features and returns its path."""
    subprocess.run(["cargo", "build", "--release", "--locked", "--quiet"], cwd=REPO, check=True)
    target = Path(os.environ.get("CARGO_TARGET_DIR", REPO / "target"))
    return target / "release" / "shardsync"


def environment_record() -> dict:
    """Hardware, software and revision details stored with every result."""
    def read(path: str) -> str:
        try:
            return Path(path).read_text()
        except OSError:
            return ""

    meminfo = read("/proc/meminfo")
    mem_total_kib = next((int(line.split()[1]) for line in meminfo.splitlines()
                          if line.startswith("MemTotal:")), None)
    revision = subprocess.run(
        ["git", "-c", f"safe.directory={REPO}", "rev-parse", "HEAD"],
        cwd=REPO, text=True, capture_output=True).stdout.strip() or "uncommitted"
    # Only tracked changes make a build differ from its revision; new result files do not.
    dirty = subprocess.run(
        ["git", "-c", f"safe.directory={REPO}", "status", "--porcelain",
         "--untracked-files=no"],
        cwd=REPO, text=True, capture_output=True).stdout.strip() != ""
    return {
        "kernel": platform.release(),
        "machine": platform.machine(),
        "cpus_visible": os.cpu_count(),
        "memory_total_bytes": mem_total_kib * 1024 if mem_total_kib else None,
        "rustc": run(["rustc", "--version"]).stdout.strip(),
        "git_revision": revision,
        "git_dirty": dirty,
        "build": "cargo build --release --locked (no optional features)",
        "host_cpu": os.environ.get("HOST_CPU", "unknown"),
        "host_os": os.environ.get("HOST_OS", "unknown"),
        "host_memory_bytes": os.environ.get("HOST_MEMORY_BYTES", "unknown"),
        "container": os.environ.get("CONTAINER_NOTE", "none"),
        "network": "loopback (sender and receiver on one machine)",
    }


def filesystem_type(path: Path) -> str:
    return run(["stat", "-f", "-c", "%T", str(path)]).stdout.strip()


# ----- Deterministic datasets ---------------------------------------------------------------

def pseudo_random_file(path: Path, size: int, seed: str) -> None:
    """Writes `size` deterministic, incompressible bytes (AES-256-CTR keystream)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as out:
        if size == 0:
            return
        encrypt = subprocess.Popen(
            ["openssl", "enc", "-aes-256-ctr", "-pbkdf2", "-nosalt", "-pass", f"pass:{seed}",
             "-in", "/dev/zero"], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        remaining = size
        while remaining:
            block = encrypt.stdout.read(min(remaining, 4 * MIB))
            out.write(block)
            remaining -= len(block)
        encrypt.kill()
        encrypt.wait()


def make_dataset(kind: str, root: Path, total_mib: int, seed: int) -> dict:
    """Creates a dataset. `single`: one file. `tree`: about 2,000 files of varied sizes plus
    empty directories and zero-byte files, totalling roughly `total_mib`."""
    if root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True)
    if kind == "single":
        pseudo_random_file(root / "dataset.bin", total_mib * MIB, f"shardsync-{seed}")
        return {"kind": kind, "files": 1, "bytes": total_mib * MIB, "seed": seed}
    rng = random.Random(seed)
    budget = total_mib * MIB
    files = 0
    # A few large files, then many small ones, spread across nested directories.
    sizes = [rng.randint(8 * MIB, 24 * MIB) for _ in range(max(1, total_mib // 64))]
    remaining = max(0, budget - sum(sizes))
    small_count = 1990 - len(sizes)
    for _ in range(small_count):
        sizes.append(rng.randint(0, max(1, 2 * remaining // small_count)))
    for index, size in enumerate(sizes):
        directory = root / f"part-{index % 20:02d}" / f"group-{index % 7}"
        pseudo_random_file(directory / f"file-{index:05d}.bin", size, f"shardsync-{seed}-{index}")
        files += 1
    for index in range(5):
        (root / f"empty-{index}").mkdir()
        (root / f"part-{index:02d}" / f"zero-{index}.bin").write_bytes(b"")
        files += 1
    actual = sum(p.stat().st_size for p in root.rglob("*") if p.is_file())
    return {"kind": kind, "files": files, "bytes": actual, "seed": seed}


def tree_digest(root: Path) -> dict:
    """Relative path -> SHA-256 for files, or 'dir' for directories."""
    result = {}
    for path in sorted(root.rglob("*")):
        relative = str(path.relative_to(root))
        if path.is_dir():
            result[relative] = "dir"
        else:
            digest = hashlib.sha256()
            with open(path, "rb") as handle:
                for block in iter(lambda: handle.read(4 * MIB), b""):
                    digest.update(block)
            result[relative] = digest.hexdigest()
    return result


# ----- Processes ----------------------------------------------------------------------------

def child_pids(parent: int) -> list[int]:
    """Direct children of `parent`, read from /proc."""
    children = []
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            fields = (entry / "stat").read_text().rsplit(")", 1)[1].split()
        except OSError:
            continue
        if int(fields[1]) == parent:
            children.append(int(entry.name))
    return children


class Receiver:
    """A receiver process on a fixed or ephemeral port."""

    def __init__(self, binary: Path, root: Path, dev: Path, log: Path, port: int = 0):
        self.process = subprocess.Popen(
            [str(binary), "receive", "--root", str(root), "--listen", f"127.0.0.1:{port}",
             "--tls-cert", str(dev / "server-cert.pem"), "--tls-key", str(dev / "server-key.pem"),
             "--tokens", str(dev / "tokens.json")],
            stdout=subprocess.PIPE, stderr=open(log, "ab"), text=True,
            env={**os.environ, "SHARDSYNC_LOG": "info"})
        line = self.process.stdout.readline()
        if not line.startswith("SHARDSYNC_LISTENING "):
            raise RuntimeError(f"receiver did not start; see {log}")
        self.port = int(line.split()[1].rsplit(":", 1)[1])
        threading.Thread(target=self.process.stdout.read, daemon=True).start()

    def peak_rss_bytes(self) -> int | None:
        try:
            for line in Path(f"/proc/{self.process.pid}/status").read_text().splitlines():
                if line.startswith("VmHWM:"):
                    return int(line.split()[1]) * 1024
        except OSError:
            return None
        return None

    def cpu_seconds(self) -> float | None:
        try:
            fields = Path(f"/proc/{self.process.pid}/stat").read_text().rsplit(")", 1)[1].split()
            ticks = os.sysconf("SC_CLK_TCK")
            return (int(fields[11]) + int(fields[12])) / ticks
        except OSError:
            return None

    def kill(self) -> None:
        self.process.send_signal(signal.SIGKILL)
        self.process.wait()

    def stop(self) -> None:
        self.process.send_signal(signal.SIGTERM)
        self.process.wait(timeout=60)


class Sender:
    """A `shardsync send` process measured by GNU time."""

    def __init__(self, binary: Path, spool: Path, port: int, dev: Path, workdir: Path, label: str):
        self.time_log = workdir / f"time-{label}.txt"
        self.report_path = workdir / f"report-{label}.json"
        self.started = time.monotonic()
        self.process = subprocess.Popen(
            ["/usr/bin/time", "-v", "-o", str(self.time_log), str(binary), "send",
             "--spool", str(spool), "--endpoint", f"https://localhost:{port}",
             "--ca", str(dev / "ca.pem"), "--token-file", str(dev / "token-demo.txt")],
            stdout=open(self.report_path, "w"), stderr=open(workdir / f"sender-{label}.log", "w"),
            env={**os.environ, "SHARDSYNC_LOG": "warn"})

    def kill(self) -> None:
        """SIGKILLs the sender itself (the child of GNU time), as a crash would."""
        for pid in child_pids(self.process.pid):
            os.kill(pid, signal.SIGKILL)
        self.process.wait()

    def wait(self) -> dict:
        code = self.process.wait()
        if code != 0:
            raise RuntimeError(f"sender exited with {code}; see {self.report_path.parent}")
        return json.loads(self.report_path.read_text())

    def resources(self) -> dict:
        text = self.time_log.read_text() if self.time_log.exists() else ""
        values = {}
        for line in text.splitlines():
            key, _, value = line.strip().partition(": ")
            values[key] = value
        def number(key: str) -> float | None:
            try:
                return float(values[key])
            except (KeyError, ValueError):
                return None
        rss_kib = number("Maximum resident set size (kbytes)")
        user, system = number("User time (seconds)"), number("System time (seconds)")
        return {
            "peak_rss_bytes": int(rss_kib * 1024) if rss_kib else None,
            "cpu_seconds": (user or 0) + (system or 0) if user is not None else None,
        }


class Api:
    """Reads receiver status and diagnostics over HTTPS with the development CA."""

    def __init__(self, dev: Path, port: int):
        self.context = ssl.create_default_context(cafile=str(dev / "ca.pem"))
        self.token = (dev / "token-demo.txt").read_text().strip()
        self.base = f"https://localhost:{port}"

    def get(self, path: str) -> dict:
        request = urllib.request.Request(self.base + path,
                                         headers={"Authorization": f"Bearer {self.token}"})
        with urllib.request.urlopen(request, context=self.context, timeout=10) as response:
            return json.loads(response.read())

    def diagnostics(self) -> dict:
        return self.get("/v1/receiver")

    def session(self, session_id: str) -> dict:
        return self.get(f"/v1/sessions/{session_id}")


def session_id_from_spool(spool: Path) -> str | None:
    try:
        return json.loads((spool / "sender-state.json").read_text()).get("session_id")
    except (OSError, ValueError):
        return None


def wait_for_durable_fraction(api: Api, spool: Path, fraction: float, timeout: float = 600) -> dict:
    """Polls until the receiver reports at least `fraction` of bytes durable."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        session = session_id_from_spool(spool)
        if session:
            status = api.session(session)
            if status["total_bytes"] and status["durable_bytes"] >= fraction * status["total_bytes"]:
                return status
        time.sleep(0.05)
    raise RuntimeError("transfer did not reach the interruption point")


def median(values: list[float]) -> float:
    ordered = sorted(values)
    middle = len(ordered) // 2
    return ordered[middle] if len(ordered) % 2 else (ordered[middle - 1] + ordered[middle]) / 2
