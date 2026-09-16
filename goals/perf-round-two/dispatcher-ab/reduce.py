#!/usr/bin/env python3
"""Reduce dispatcher A/B runs.csv into per-cell median/IQR markdown tables.

Warm-up rows (pos=warmup) are excluded; failed runs (exit!=0) are excluded
from statistics but reported. Every cell is emitted — no favorable-case
selection. Usage: reduce.py OUTDIR/runs.csv > table.md
"""

import csv
import statistics
import sys
from collections import defaultdict

MEASURES = [
    ("achieved_per_s", "achieved it/s", 1.0),
    ("dropped", "dropped", 1.0),
    ("wall_s", "wall s", 1.0),
    ("task_clock_ms", "task-clock ms", 1.0),
    ("cycles", "Gcycles", 1e9),
    ("instructions", "Ginstr", 1e9),
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
            cells[cell][row["binary"]].append(row)

    print("# Dispatcher A/B: per-cell medians (IQR = p25..p75), 5 measured runs/side\n")
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
            f"rate={meta['rate']}/s think={meta['think']} tick={meta['tick_us']}us "
            f"worker-threads={meta['worker_threads']} pre/max VUs={meta['pre_vus']}/{meta['max_vus']} "
            f"cores={meta['cores']} (n base={len(base)}, cand={len(cand)})\n"
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
        target = float(meta["rate"])
        bm = statistics.median([float(r["achieved_per_s"]) for r in base])
        cm = statistics.median([float(r["achieved_per_s"]) for r in cand])
        if bm < 0.9 * target and cm < 0.9 * target:
            print(f"\n_Note: both sides below 90% of the {target:.0f}/s target — host-limited cell._")
        print()


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "runs.csv")
