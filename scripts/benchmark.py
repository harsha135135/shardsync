"""ShardSync measurements (§11): uninterrupted and interrupted transfers of the same
deterministic datasets, repeated, with raw per-run results and medians.

For each dataset (one large file; a tree of about 2,000 files) and each scenario
(uninterrupted; SIGKILL both processes at about 50% durable bytes, then restart both), it
records payload throughput, preparation-to-publication time, retransmitted bytes, CPU, peak
RSS, startup recheck cost, and the receiver's flush, commit, verification and publication
time. Run it through `scripts/benchmark.sh`. There are no targets to meet: the numbers
describe this machine and configuration only.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import time
from pathlib import Path

from harness import (REPO, Api, Receiver, Sender, build_release, environment_record,
                     filesystem_type, make_dataset, median, run, tree_digest,
                     wait_for_durable_fraction)

COUNTERS = ("chunk_sync_micros", "receipt_commit_micros", "verify_micros", "publish_micros",
            "chunk_writes", "duplicate_chunk_requests")


def one_run(binary: Path, source: Path, work: Path, interrupted: bool) -> dict:
    if work.exists():
        shutil.rmtree(work)
    work.mkdir(parents=True)
    dev, spool, root = work / "dev", work / "spool", work / "receiver"
    run([str(binary), "dev-setup", "--out", str(dev)])
    receiver = Receiver(binary, root, dev, work / "receiver.log")
    port, api = receiver.port, Api(dev, receiver.port)
    assert not api.diagnostics()["fault_injection_enabled"]

    started = time.monotonic()
    prepare = json.loads(run([str(binary), "prepare", str(source), "--spool", str(spool)]).stdout)
    prepare_ms = (time.monotonic() - started) * 1000

    counters = {key: 0 for key in COUNTERS}
    receiver_peaks, receiver_cpu, sender_peaks, sender_cpu = [], 0.0, [], 0.0
    recovery = None
    send_started = time.monotonic()
    first = Sender(binary, spool, port, dev, work, "first")
    if interrupted:
        wait_for_durable_fraction(api, spool, 0.5)
        before = api.diagnostics()
        receiver_peaks.append(receiver.peak_rss_bytes())
        receiver_cpu += receiver.cpu_seconds() or 0
        receiver.kill()
        first.kill()
        for key in COUNTERS:
            counters[key] += before[key]
        resources = first.resources()
        sender_peaks.append(resources["peak_rss_bytes"])
        sender_cpu += resources["cpu_seconds"] or 0
        receiver = Receiver(binary, root, dev, work / "receiver.log", port=port)
        recovery = api.diagnostics()["recovery"]
        final = Sender(binary, spool, port, dev, work, "second")
    else:
        final = first
    report = final.wait()
    send_ms = (time.monotonic() - send_started) * 1000
    total_ms = (time.monotonic() - started) * 1000
    end = api.diagnostics()
    for key in COUNTERS:
        counters[key] += end[key]
    receiver_peaks.append(end["peak_rss_bytes"])
    receiver_cpu += receiver.cpu_seconds() or 0
    resources = final.resources()
    sender_peaks.append(resources["peak_rss_bytes"])
    sender_cpu += resources["cpu_seconds"] or 0
    receiver.stop()

    published = root / "completed" / report["session_id"] / "data"
    identical = tree_digest(published) == tree_digest(spool / "content")
    result = {
        "scenario": "interrupted" if interrupted else "uninterrupted",
        "identical": identical,
        "total_bytes": report["total_bytes"],
        "prepare_ms": prepare_ms,
        "prepare_copy_ms": prepare["copy_millis"],
        "prepare_hash_ms": prepare["hash_millis"],
        "send_ms": send_ms,
        "payload_mib_per_s": report["total_bytes"] / 1024 / 1024 / (send_ms / 1000),
        "preparation_to_publication_ms": total_ms,
        "commit_to_published_ms": report["commit_to_published_millis"],
        "resent_bytes": report["resent_bytes_all_runs"],
        "sender_cpu_seconds": sender_cpu,
        "receiver_cpu_seconds": receiver_cpu,
        "sender_peak_rss_bytes": max(p for p in sender_peaks if p),
        "receiver_peak_rss_bytes": max(p for p in receiver_peaks if p),
        "recovery": recovery,
        **{f"receiver_{key}": value for key, value in counters.items()},
    }
    shutil.rmtree(work)
    if not identical:
        raise SystemExit("published output does not match the spool")
    return result


def summarize(runs: list[dict]) -> dict:
    keys = ["payload_mib_per_s", "send_ms", "prepare_ms", "preparation_to_publication_ms",
            "commit_to_published_ms", "resent_bytes", "sender_cpu_seconds",
            "receiver_cpu_seconds", "sender_peak_rss_bytes", "receiver_peak_rss_bytes",
            "receiver_chunk_sync_micros", "receiver_receipt_commit_micros",
            "receiver_verify_micros", "receiver_publish_micros"]
    summary = {}
    for key in keys:
        values = [run[key] for run in runs if run.get(key) is not None]
        if values:
            summary[key] = {"median": median(values), "min": min(values), "max": max(values)}
    recoveries = [run["recovery"] for run in runs if run["recovery"]]
    if recoveries:
        for key in ("bytes_rehashed", "duration_millis"):
            values = [r[key] for r in recoveries]
            summary[f"recovery_{key}"] = {"median": median(values), "min": min(values),
                                          "max": max(values)}
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work", default=os.environ.get("SHARDSYNC_WORK", "/work/testfs/bench"))
    parser.add_argument("--single-mib", type=int, default=1024)
    parser.add_argument("--tree-mib", type=int, default=512)
    parser.add_argument("--repetitions", type=int, default=5)
    parser.add_argument("--seed", type=int, default=11)
    parser.add_argument("--results", default=str(REPO / "benchmarks" / "results"))
    args = parser.parse_args()

    base = Path(args.work)
    if base.exists():
        shutil.rmtree(base)
    base.mkdir(parents=True)
    binary = build_release()
    record = {
        "kind": "benchmark",
        "recorded_at_unix": int(time.time()),
        "environment": environment_record(),
        "filesystem": filesystem_type(base),
        "settings": {"repetitions": args.repetitions, "upload_workers": 4,
                     "chunk_buffer_permits": 8, "chunk_size_bytes": 4 * 1024 * 1024,
                     "interrupt_at_durable_fraction": 0.5, "warm_up": "none; every run "
                     "starts a fresh receiver, spool and receiver root"},
        "datasets": [],
    }
    for kind, size in (("single", args.single_mib), ("tree", args.tree_mib)):
        source = base / f"source-{kind}"
        dataset = make_dataset(kind, source, size, args.seed)
        print(f"{kind}: {dataset['files']} files, {dataset['bytes'] / 1024 / 1024:,.1f} MiB")
        if kind == "single":
            source = source / "dataset.bin"
        entry = {"dataset": dataset, "scenarios": {}}
        for interrupted in (False, True):
            name = "interrupted" if interrupted else "uninterrupted"
            runs = []
            for repetition in range(args.repetitions):
                result = one_run(binary, source, base / "run", interrupted)
                result["repetition"] = repetition
                runs.append(result)
                print(f"  {name} #{repetition + 1}: {result['payload_mib_per_s']:,.1f} MiB/s, "
                      f"resent {result['resent_bytes'] / 1024 / 1024:,.1f} MiB, "
                      f"receiver RSS {result['receiver_peak_rss_bytes'] / 1024 / 1024:,.1f} MiB")
            entry["scenarios"][name] = {"runs": runs, "summary": summarize(runs)}
        record["datasets"].append(entry)
        shutil.rmtree(base / f"source-{kind}")

    results = Path(args.results)
    results.mkdir(parents=True, exist_ok=True)
    out = results / f"benchmark-{time.strftime('%Y%m%dT%H%M%S')}.json"
    out.write_text(json.dumps(record, indent=2) + "\n")
    print(f"raw results: {out}")
    shutil.rmtree(base)


if __name__ == "__main__":
    main()
