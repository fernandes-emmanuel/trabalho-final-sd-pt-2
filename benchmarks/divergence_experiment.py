"""Measures how often replicas diverge under FIFO-only ordering as network latency grows."""

from __future__ import annotations

import argparse
import json
import statistics
import tempfile
from pathlib import Path

from boxoffice.domain.workload import WorkloadSpec
from boxoffice.launcher import LocalDeployment, run_local
from mpcomm.transport.faults import FaultProfile


def run_scenario(delay_ms: float, args: argparse.Namespace, workdir: Path) -> dict:
    report_path = workdir / f"delay-{delay_ms}.json"
    deployment = LocalDeployment(
        offices=args.offices,
        sessions=args.sessions,
        workload=WorkloadSpec(commands=args.commands, seed=args.seed),
        base_port=args.base_port,
        faults=FaultProfile(delay_ms=delay_ms, jitter_ms=delay_ms, seed=args.seed),
        report=report_path,
        log_level="warning",
    )
    exit_code = run_local(deployment)
    reports = json.loads(report_path.read_text())
    return {
        "delay_ms": delay_ms,
        "exit_code": exit_code,
        "sessions": len(reports),
        "complete": sum(report["complete"] for report in reports),
        "consistent": sum(report["consistent"] for report in reports),
        "distinct_states": statistics.fmean(report["distinct_states"] for report in reports),
        "divergent_seats": statistics.fmean(len(report["seat_conflicts"]) for report in reports),
        "overbooked_seats": statistics.fmean(report["overbooked_seats"] for report in reports),
        "divergent_outcomes": statistics.fmean(
            len(report["outcome_conflicts"]) for report in reports
        ),
    }


def to_markdown(rows: list[dict], commands: int, offices: int) -> str:
    lines = [
        f"| Delay +/- jitter (ms) | Complete | Consistent | Distinct states | Divergent seats "
        f"| Overbooked seats | Divergent outcomes (of {commands * offices}) |",
        "|---:|---:|---:|---:|---:|---:|---:|",
    ]
    lines += [
        f"| {row['delay_ms']:.0f} +/- {row['delay_ms']:.0f} "
        f"| {row['complete']}/{row['sessions']} | {row['consistent']}/{row['sessions']} "
        f"| {row['distinct_states']:.1f} | {row['divergent_seats']:.1f} "
        f"| {row['overbooked_seats']:.1f} | {row['divergent_outcomes']:.1f} |"
        for row in rows
    ]
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--delays", type=float, nargs="+", default=[0, 5, 20, 60])
    parser.add_argument("--offices", type=int, default=5)
    parser.add_argument("--sessions", type=int, default=10)
    parser.add_argument("--commands", type=int, default=40)
    parser.add_argument("--seed", type=int, default=2024)
    parser.add_argument("--base-port", type=int, default=7700)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory() as workdir:
        rows = [run_scenario(delay, args, Path(workdir)) for delay in args.delays]
    print(to_markdown(rows, args.commands, args.offices))


if __name__ == "__main__":
    main()
