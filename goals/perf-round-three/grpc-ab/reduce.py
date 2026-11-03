#!/usr/bin/env python3
"""Reduce gRPC encode-once A/B runs.csv into per-cell median/IQR tables.

Warm-up rows (pos=warmup) are excluded; failed runs (exit!=0) are excluded
from statistics but reported. Every cell is emitted — no favorable-case
selection. `instr/iter` is derived per run before aggregation, so it is
robust to small achieved-rate differences between the sides.
Usage: reduce.py OUTDIR/runs.csv > table.md
"""

import csv
import statistics
import sys
from collections import defaultdict

MEASURES = [
    ("achieved_per_s", "achieved it/s", 1.0),
    ("wall_s", "wall s", 1.0),
    ("task_clock_ms", "task-clock ms", 1.0),
    ("cycles", "Gcycles", 1e9),
    ("instructions", "Ginstr", 1e9),
    ("instr_per_iter", "instr/iter", 1.0),
    ("ctx_switches", "ctx-sw", 1.0),
    ("cpu_migrations", "migrations", 1.0),
    ("cache_misses", "Mcache-miss", 1e6),
]


def q(vals, p):
    vals = sorted(vals)
    if not vals:
        return float("nan")
    k = (len(vals) - 1) * p
    lo, hi = int(k), min(int(k) + 1, len(vals) - 1)
    return vals[lo] + (vals[hi] - vals[lo]) * (k - lo)


def fmt(v):
    if v != v:  # NaN
        return "-"
    if abs(v) >= 1000:
        return f"{v:,.0f}"
    if abs(v) >= 10:
        return f"{v:.1f}"
    return f"{v:.3f}"


def main(path):
    cells = defaultdict(lambda: defaultdict(list))
    failures = []
    order = []
    with open(path) as f:
        for row in csv.DictReader(f):
            if row["pos"] == "warmup":
                continue
            cell = row["cell"]
            if cell not in order:
                order.append(cell)
            if row["exit"] != "0":
                failures.append(f'{cell} {row["binary"]} pair={row["pair"]} exit={row["exit"]}')
                continue
            try:
                row["instr_per_iter"] = str(float(row["instructions"]) / float(row["iterations"]))
            except (ValueError, ZeroDivisionError):
                row["instr_per_iter"] = ""
            cells[cell][row["binary"]].append(row)

    print("# gRPC encode-once A/B: per-cell medians (IQR = p25..p75)\n")
    if failures:
        print("**Failed runs (excluded from stats):** " + "; ".join(failures) + "\n")
    for cell in order:
        sides = cells[cell]
        base, cand = sides.get("base", []), sides.get("cand", [])
        if not base or not cand:
            print(f"## {cell}\n\nmissing side: base={len(base)} cand={len(cand)} runs\n")
            continue
        meta = base[0]
        print(
            f"## {cell}\n\n"
            f"method={meta['method']} payload={meta['payload_bytes']}B "
            f"messages/call={meta['messages']} vus={meta['vus']} cores={meta['cores']} "
            f"(n base={len(base)}, cand={len(cand)})\n"
        )
        print("| measure | base med | base IQR | cand med | cand IQR | Δ med |")
        print("|---|---|---|---|---|---|")
        for key, label, scale in MEASURES:
            b = [float(r[key]) / scale for r in base if r[key] not in ("", None)]
            c = [float(r[key]) / scale for r in cand if r[key] not in ("", None)]
            if not b or not c:
                print(f"| {label} | - | - | - | - | - |")
                continue
            bm, cm = statistics.median(b), statistics.median(c)
            delta = "-" if bm == 0 else f"{(cm - bm) / bm * 100:+.1f}%"
            print(
                f"| {label} | {fmt(bm)} | {fmt(q(b, 0.25))}..{fmt(q(b, 0.75))} "
                f"| {fmt(cm)} | {fmt(q(c, 0.25))}..{fmt(q(c, 0.75))} | {delta} |"
            )
        print()


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "runs.csv")
