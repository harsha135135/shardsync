"""The ShardSync demonstration (§1): interrupt a large transfer, restart both processes,
resume, and prove that the published output exactly matches the prepared input.

It reports how many bytes were resent, peak memory, the restart recheck cost, and the time
spent preparing, transferring, verifying and publishing. Run it through `scripts/demo.sh`.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import time
from pathlib import Path

from harness import (REPO, Api, Receiver, Sender, build_release, environment_record,
                     filesystem_type, make_dataset, run, tree_digest, wait_for_durable_fraction)


def mib(value: float | int | None) -> str:
    return "n/a" if value is None else f"{value / 1024 / 1024:,.1f} MiB"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work", default=os.environ.get("SHARDSYNC_WORK", "/work/testfs/demo"))
    parser.add_argument("--dataset", choices=["single", "tree"], default="tree")
    parser.add_argument("--size-mib", type=int, default=1024)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--interrupt-at", type=float, default=0.5)
    parser.add_argument("--results", default=str(REPO / "benchmarks" / "results"))
    args = parser.parse_args()

    work = Path(args.work)
    if work.exists():
        shutil.rmtree(work)
    work.mkdir(parents=True)
    dev, spool, root = work / "dev", work / "spool", work / "receiver"

    print("1. Building the release binary (no optional features)")
    binary = build_release()
    print(f"2. Generating a deterministic {args.dataset} dataset (seed {args.seed})")
    dataset = make_dataset(args.dataset, work / "source", args.size_mib, args.seed)
    print(f"   {dataset['files']} files, {mib(dataset['bytes'])}, on {filesystem_type(work)}")
    run([str(binary), "dev-setup", "--out", str(dev)])

    receiver = Receiver(binary, root, dev, work / "receiver.log")
    port = receiver.port
    api = Api(dev, port)
    diagnostics = api.diagnostics()
    assert not diagnostics["fault_injection_enabled"], "measurements need a build without faults"
    print(f"3. Receiver listening on port {port}; durability profile "
          f"{diagnostics['durability_profile']} ({diagnostics['filesystem']})")

    started = time.monotonic()
    prepare = json.loads(run([str(binary), "prepare", str(work / "source"),
                              "--spool", str(spool)]).stdout)
    prepare_ms = (time.monotonic() - started) * 1000
    print(f"4. Prepared the spool in {prepare_ms:,.0f} ms "
          f"(copy {prepare['copy_millis']} ms, hash {prepare['hash_millis']} ms)")

    send_started = time.monotonic()
    first = Sender(binary, spool, port, dev, work, "first")
    status = wait_for_durable_fraction(api, spool, args.interrupt_at)
    receiver_peak_first = receiver.peak_rss_bytes()
    receiver_cpu_first = receiver.cpu_seconds()
    receiver.kill()
    first.kill()
    first_send_ms = (time.monotonic() - send_started) * 1000
    durable_fraction = status["durable_bytes"] / status["total_bytes"]
    visible = list((root / "completed").iterdir())
    print(f"5. SIGKILLed the receiver and the sender at {durable_fraction:.0%} durable bytes; "
          f"completed/ holds {len(visible)} entries (incomplete output stays private)")

    restart_started = time.monotonic()
    receiver = Receiver(binary, root, dev, work / "receiver.log", port=port)
    restart_ms = (time.monotonic() - restart_started) * 1000
    recovery = api.diagnostics()["recovery"]
    print(f"6. Restarted the receiver in {restart_ms:,.0f} ms; startup recheck read "
          f"{mib(recovery['bytes_rehashed'])} in {recovery['duration_millis']} ms and cleared "
          f"{recovery['receipts_cleared']} receipts")

    resume_started = time.monotonic()
    second = Sender(binary, spool, port, dev, work, "second")
    report = second.wait()
    resume_ms = (time.monotonic() - resume_started) * 1000
    total_ms = (time.monotonic() - started) * 1000
    end = api.diagnostics()
    receiver_cpu_second = receiver.cpu_seconds()
    receiver.stop()
    print(f"7. Restarted the sender; it resumed and the receiver published "
          f"{report['destination']}")

    published = root / "completed" / report["session_id"] / "data"
    spool_digest = tree_digest(spool / "content")
    published_digest = tree_digest(published)
    source_digest = tree_digest(work / "source")
    identical = spool_digest == published_digest == source_digest
    print(f"8. Published tree equals the prepared spool and the source: {identical} "
          f"({len(published_digest)} entries compared by SHA-256)")
    if not identical:
        raise SystemExit("published output does not match")

    first_resources, second_resources = first.resources(), second.resources()
    summary = {
        "kind": "demo",
        "recorded_at_unix": int(time.time()),
        "environment": environment_record(),
        "filesystem": filesystem_type(work),
        "durability_profile": end["durability_profile"],
        "dataset": dataset,
        "interrupted_at_durable_fraction": durable_fraction,
        "identical": identical,
        "total_bytes": report["total_bytes"],
        "resent_bytes": report["resent_bytes_all_runs"],
        "attempted_bytes_all_runs": report["attempted_bytes_all_runs"],
        "prepare_ms": prepare_ms,
        "first_send_until_kill_ms": first_send_ms,
        "receiver_restart_ms": restart_ms,
        "resume_until_published_ms": resume_ms,
        "commit_to_published_ms": report["commit_to_published_millis"],
        "preparation_to_publication_ms": total_ms,
        "recovery": recovery,
        "receiver_counters_after_restart": {key: end[key] for key in (
            "chunk_writes", "duplicate_chunk_requests", "chunk_sync_micros",
            "receipt_commit_micros", "verify_micros", "publish_micros", "buffer_high_water")},
        "receiver_peak_rss_bytes": {"before_kill": receiver_peak_first,
                                    "after_restart": end["peak_rss_bytes"]},
        "receiver_cpu_seconds": {"before_kill": receiver_cpu_first,
                                 "after_restart": receiver_cpu_second},
        "sender_peak_rss_bytes": {"first": first_resources["peak_rss_bytes"],
                                  "second": second_resources["peak_rss_bytes"]},
        "sender_cpu_seconds": {"first": first_resources["cpu_seconds"],
                               "second": second_resources["cpu_seconds"]},
    }
    results = Path(args.results)
    results.mkdir(parents=True, exist_ok=True)
    out = results / f"demo-{time.strftime('%Y%m%dT%H%M%S')}.json"
    out.write_text(json.dumps(summary, indent=2) + "\n")

    print("\nSummary")
    print(f"  content            {mib(report['total_bytes'])}")
    print(f"  resent             {mib(report['resent_bytes_all_runs'])}")
    print(f"  receiver peak RSS  {mib(receiver_peak_first)} before kill, "
          f"{mib(end['peak_rss_bytes'])} after restart")
    print(f"  sender peak RSS    {mib(first_resources['peak_rss_bytes'])} first run, "
          f"{mib(second_resources['peak_rss_bytes'])} second run")
    print(f"  verification       {end['verify_micros'] / 1000:,.0f} ms reading and hashing "
          f"after restart; commit to published {report['commit_to_published_millis']} ms")
    print(f"  end to end         {total_ms / 1000:,.1f} s from preparation to publication, "
          f"including the interruption")
    print(f"  raw result         {out.relative_to(REPO) if out.is_relative_to(REPO) else out}")
    shutil.rmtree(work)


if __name__ == "__main__":
    main()
