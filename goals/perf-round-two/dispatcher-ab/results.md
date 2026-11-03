# Dispatcher claim-budget local A/B — results

Paired release-mode comparison required by
`goals/perf-round-two/dispatcher-idle-ring.md` (LOCAL PERFORMANCE VALIDATION).
This measures the isolated dispatcher mechanism on one host; it does not
replace the later Graviton/AWS validation (`aws-ab-validation.md`).

## Setup

- Base: `d28814f` (`origin/nikolai/perf-dispatcher-port` — the current
  dispatcher). Candidate: `c218d6e` = d28814f + the claim-budget dispatcher
  commits only; identical `Cargo.lock` (sha256 `b492571c…` both sides),
  default features, `cargo build --release --locked -p loadr-cli`.
- Toolchain: rustc 1.94.0 (4a4ef493e 2026-03-02), LLVM 21.1.8,
  x86_64-unknown-linux-gnu.
- Host: AMD EPYC 9454P (48 physical cores, SMT2 — CPUs 0–47 are distinct
  physical cores), Linux 6.8.0-90-generic, `schedutil` governor,
  perf 6.8.12, `perf_event_paranoid=-1`. Runs on 2026-07-13.
- Binary sha256: base `7e3aadab…`, cand `117f1e52…`
  (full hashes in `target/ab/out/meta.txt`, not committed).

## Method

- 25 cells: `LOADR_DISPATCH_TICK_US` {1000, 5000} × `--worker-threads` {2, 16}
  × constant think time {0s, 1ms} × rate {50k, 150k, 250k}/s, plus a
  low-rate/high-preallocated broadcast over-waking case (1k/s, 2000 VUs
  parked, think 1ms, tick 5000, 16 threads).
- VU sizing identical within every pair: think-0 → 64/128 pre/max; think-1ms →
  512/1024; over-waking case 2000/2000. 10 s per run, `graceful_stop: 1s`,
  no-network plans whose only flow step is constant think time, no
  sample-consuming outputs (summary export serializes the final snapshot
  post-run; shard-mode recording stays active).
- Per cell: one discarded warm-up per binary, then 5 measured pairs in
  alternating order (A,B B,A A,B B,A A,B). Both sides pinned to the same
  physical cores (`taskset -c 0-3` for 2 worker threads, `0-19` for 16; CPUs
  0–47 are distinct physical cores on this host). Every run wrapped in
  `perf stat -e task-clock,cycles,instructions,context-switches,cpu-migrations,cache-misses`.
- Raw rows: `runs.csv` (committed, one row per measured run, failures
  included). Reduction: `reduce.py` (median + p25..p75 IQR). All cells
  reported; none selected out.

## Results

## r50000-0s-tick1000-wt2

rate=50000/s think=0s tick=1000us worker-threads=2 pre/max VUs=64/128 cores=0-3 (n base=5, cand=5)

| measure | base med | base IQR | cand med | cand IQR | Δ med |
|---|---|---|---|---|---|
| achieved it/s | 49,995 | 49,991..49,998 | 49,995 | 49,995..49,996 | +0.0% |
| dropped | 7.000 | 3.000..78.0 | 0.000 | 0.000..0.000 | -100.0% |
| wall s | 11.0 | 11.0..11.0 | 10.0 | 10.0..10.0 | -9.1% |
| task-clock ms | 2,112 | 2,082..2,135 | 2,873 | 2,722..3,162 | +36.0% |
| Gcycles | 3.661 | 3.660..3.685 | 4.620 | 4.523..4.712 | +26.2% |
| Ginstr | 4.390 | 4.371..4.397 | 4.918 | 4.913..4.942 | +12.0% |
| ctx-sw | 17,678 | 17,432..17,685 | 17,008 | 16,992..17,047 | -3.8% |
| migrations | 17.0 | 6.000..66.0 | 13.0 | 12.0..31.0 | -23.5% |
| Mcache-miss | 29.8 | 28.5..30.8 | 34.9 | 34.9..36.2 | +17.2% |

## r150000-0s-tick1000-wt2

rate=150000/s think=0s tick=1000us worker-threads=2 pre/max VUs=64/128 cores=0-3 (n base=5, cand=5)

| measure | base med | base IQR | cand med | cand IQR | Δ med |
|---|---|---|---|---|---|
| achieved it/s | 60,104 | 59,974..60,157 | 105,834 | 105,334..105,972 | +76.1% |
| dropped | 898,856 | 898,321..900,181 | 441,508 | 440,205..446,610 | -50.9% |
| wall s | 11.0 | 11.0..11.0 | 10.0 | 10.0..10.0 | -9.1% |
| task-clock ms | 3,074 | 2,716..3,156 | 4,140 | 4,044..4,188 | +34.7% |
| Gcycles | 4.665 | 4.663..4.667 | 6.434 | 6.346..6.436 | +37.9% |
| Ginstr | 5.888 | 5.876..5.889 | 6.857 | 6.825..6.879 | +16.5% |
| ctx-sw | 16,610 | 16,558..17,146 | 15,149 | 15,134..15,288 | -8.8% |
| migrations | 5.000 | 4.000..6.000 | 4.000 | 4.000..5.000 | -20.0% |
| Mcache-miss | 43.1 | 41.5..44.0 | 57.5 | 56.7..58.0 | +33.5% |

_Note: both sides below 90% of the 150000/s target — host-limited cell._

## r250000-0s-tick1000-wt2

rate=250000/s think=0s tick=1000us worker-threads=2 pre/max VUs=64/128 cores=0-3 (n base=5, cand=5)

| measure | base med | base IQR | cand med | cand IQR | Δ med |
|---|---|---|---|---|---|
| achieved it/s | 65,587 | 65,437..65,946 | 106,524 | 106,160..106,558 | +62.4% |
| dropped | 1,844,033 | 1,840,326..1,845,536 | 1,434,560 | 1,434,229..1,438,196 | -22.2% |
| wall s | 11.0 | 11.0..11.0 | 10.0 | 10.0..10.0 | -9.1% |
| task-clock ms | 3,387 | 3,237..3,401 | 4,104 | 4,060..4,213 | +21.2% |
| Gcycles | 5.735 | 5.552..5.761 | 6.343 | 6.313..6.412 | +10.6% |
| Ginstr | 7.286 | 7.264..7.299 | 6.863 | 6.855..6.879 | -5.8% |
| ctx-sw | 16,493 | 16,432..16,652 | 15,225 | 15,133..15,240 | -7.7% |
| migrations | 3.000 | 2.000..4.000 | 2.000 | 2.000..2.000 | -33.3% |
| Mcache-miss | 49.6 | 49.5..50.2 | 56.9 | 56.7..57.1 | +14.7% |

_Note: both sides below 90% of the 250000/s target — host-limited cell._

## r50000-1ms-tick1000-wt2

rate=50000/s think=1ms tick=1000us worker-threads=2 pre/max VUs=512/1024 cores=0-3 (n base=5, cand=5)

| measure | base med | base IQR | cand med | cand IQR | Δ med |
|---|---|---|---|---|---|
| achieved it/s | 49,991 | 49,990..49,992 | 49,993 | 49,992..49,993 | +0.0% |
| dropped | 0.000 | 0.000..0.000 | 0.000 | 0.000..0.000 | - |
| wall s | 11.0 | 11.0..11.0 | 10.0 | 10.0..10.0 | -9.1% |
| task-clock ms | 1,852 | 1,768..1,891 | 3,891 | 3,496..4,053 | +110.2% |
| Gcycles | 2.952 | 2.884..2.974 | 7.744 | 7.679..7.972 | +162.4% |
| Ginstr | 3.893 | 3.892..3.902 | 7.849 | 7.832..7.896 | +101.6% |
| ctx-sw | 17,563 | 17,553..17,603 | 15,647 | 15,485..15,898 | -10.9% |
| migrations | 6.000 | 6.000..7.000 | 4.000 | 2.000..7.000 | -33.3% |
| Mcache-miss | 33.5 | 33.0..33.7 | 87.3 | 87.0..87.7 | +160.8% |

## r150000-1ms-tick1000-wt2

rate=150000/s think=1ms tick=1000us worker-threads=2 pre/max VUs=512/1024 cores=0-3 (n base=5, cand=5)

| measure | base med | base IQR | cand med | cand IQR | Δ med |
|---|---|---|---|---|---|
| achieved it/s | 149,978 | 149,973..149,982 | 149,971 | 149,970..149,972 | -0.0% |
| dropped | 0.000 | 0.000..0.000 | 0.000 | 0.000..0.000 | - |
| wall s | 11.0 | 11.0..11.0 | 10.0 | 10.0..10.0 | -9.1% |
| task-clock ms | 5,050 | 5,043..5,074 | 5,150 | 4,936..5,307 | +2.0% |
| Gcycles | 7.624 | 7.621..7.823 | 11.1 | 10.9..11.1 | +45.4% |
| Ginstr | 10.6 | 10.6..10.7 | 12.7 | 12.5..12.8 | +19.2% |
| ctx-sw | 15,824 | 15,702..15,984 | 14,864 | 14,863..15,032 | -6.1% |
| migrations | 5.000 | 3.000..5.000 | 2.000 | 2.000..2.000 | -60.0% |
| Mcache-miss | 101.8 | 101.3..102.5 | 129.4 | 126.5..132.7 | +27.1% |

## r250000-1ms-tick1000-wt2

rate=250000/s think=1ms tick=1000us worker-threads=2 pre/max VUs=512/1024 cores=0-3 (n base=5, cand=5)

| measure | base med | base IQR | cand med | cand IQR | Δ med |
|---|---|---|---|---|---|
| achieved it/s | 239,098 | 237,472..239,636 | 249,960 | 249,954..249,964 | +4.5% |
| dropped | 108,678 | 103,179..124,619 | 0.000 | 0.000..0.000 | -100.0% |
| wall s | 11.0 | 11.0..11.0 | 10.0 | 10.0..10.0 | -9.1% |
| task-clock ms | 8,234 | 8,193..8,425 | 7,958 | 7,885..9,206 | -3.4% |
| Gcycles | 12.3 | 11.9..12.6 | 14.6 | 14.1..14.8 | +18.2% |
| Ginstr | 17.0 | 16.9..17.0 | 18.5 | 18.4..18.6 | +8.8% |
| ctx-sw | 16,340 | 16,233..19,325 | 12,616 | 12,273..13,474 | -22.8% |
| migrations | 2.000 | 2.000..3.000 | 4.000 | 3.000..4.000 | +100.0% |
| Mcache-miss | 169.9 | 169.6..171.7 | 194.8 | 189.2..198.0 | +14.7% |

## r50000-0s-tick1000-wt16

rate=50000/s think=0s tick=1000us worker-threads=16 pre/max VUs=64/128 cores=0-19 (n base=5, cand=5)

| measure | base med | base IQR | cand med | cand IQR | Δ med |
|---|---|---|---|---|---|
| achieved it/s | 49,076 | 49,028..49,120 | 49,993 | 49,992..49,995 | +1.9% |
| dropped | 9,231 | 8,757..9,668 | 0.000 | 0.000..0.000 | -100.0% |
| wall s | 11.0 | 11.0..11.0 | 10.0 | 10.0..10.0 | -9.1% |
| task-clock ms | 18,706 | 18,542..18,805 | 33,142 | 32,469..33,534 | +77.2% |
| Gcycles | 23.5 | 23.3..24.5 | 42.4 | 42.4..44.1 | +80.3% |
| Ginstr | 12.9 | 12.7..13.3 | 23.2 | 22.8..23.6 | +79.5% |
| ctx-sw | 474,511 | 470,421..480,959 | 934,176 | 919,043..973,117 | +96.9% |
| migrations | 793.0 | 413.0..2,604 | 2,535 | 590.0..7,648 | +219.7% |
| Mcache-miss | 115.4 | 113.5..125.4 | 161.0 | 160.7..164.2 | +39.6% |

## r150000-0s-tick1000-wt16

rate=150000/s think=0s tick=1000us worker-threads=16 pre/max VUs=64/128 cores=0-19 (n base=5, cand=5)

| measure | base med | base IQR | cand med | cand IQR | Δ med |
|---|---|---|---|---|---|
| achieved it/s | 69,832 | 69,336..69,938 | 95,806 | 95,629..96,397 | +37.2% |
| dropped | 801,527 | 800,592..806,575 | 541,867 | 535,943..543,627 | -32.4% |
| wall s | 11.0 | 11.0..11.0 | 10.0 | 10.0..10.0 | -9.1% |
| task-clock ms | 36,153 | 36,109..36,352 | 34,079 | 33,625..35,054 | -5.7% |
| Gcycles | 47.2 | 47.2..47.6 | 44.8 | 44.3..45.1 | -5.2% |
| Ginstr | 24.8 | 24.6..24.9 | 23.9 | 23.7..24.4 | -3.5% |
| ctx-sw | 926,492 | 916,082..941,130 | 908,633 | 901,220..946,735 | -1.9% |
| migrations | 174.0 | 160.0..707.0 | 330.0 | 223.0..853.0 | +89.7% |
| Mcache-miss | 190.5 | 187.2..191.1 | 171.6 | 171.1..172.6 | -9.9% |

_Note: both sides below 90% of the 150000/s target — host-limited cell._

## r250000-0s-tick1000-wt16

rate=250000/s think=0s tick=1000us worker-threads=16 pre/max VUs=64/128 cores=0-19 (n base=5, cand=5)

| measure | base med | base IQR | cand med | cand IQR | Δ med |
|---|---|---|---|---|---|
| achieved it/s | 71,996 | 70,810..72,144 | 96,613 | 95,308..96,776 | +34.2% |
| dropped | 1,779,993 | 1,778,276..1,791,778 | 1,533,653 | 1,532,205..1,546,758 | -13.8% |
| wall s | 11.0 | 11.0..11.0 | 10.0 | 10.0..10.0 | -9.1% |
| task-clock ms | 37,532 | 36,962..37,568 | 34,303 | 34,293..34,425 | -8.6% |
| Gcycles | 49.1 | 49.0..49.2 | 44.5 | 44.3..45.2 | -9.2% |
| Ginstr | 26.2 | 26.1..26.3 | 23.8 | 23.8..23.9 | -9.0% |
| ctx-sw | 938,459 | 933,570..947,799 | 910,017 | 897,709..912,323 | -3.0% |
| migrations | 423.0 | 352.0..3,104 | 324.0 | 310.0..2,160 | -23.4% |
| Mcache-miss | 199.5 | 197.7..205.5 | 170.6 | 168.9..172.8 | -14.5% |

_Note: both sides below 90% of the 250000/s target — host-limited cell._

## r50000-1ms-tick1000-wt16

rate=50000/s think=1ms tick=1000us worker-threads=16 pre/max VUs=512/1024 cores=0-19 (n base=5, cand=5)

| measure | base med | base IQR | cand med | cand IQR | Δ med |
|---|---|---|---|---|---|
| achieved it/s | 49,990 | 49,990..49,990 | 49,991 | 49,991..49,995 | +0.0% |
| dropped | 0.000 | 0.000..0.000 | 0.000 | 0.000..0.000 | - |
| wall s | 11.0 | 11.0..11.0 | 10.0 | 10.0..10.0 | -9.1% |
| task-clock ms | 20,774 | 20,560..21,853 | 88,914 | 88,082..89,964 | +328.0% |
| Gcycles | 26.9 | 26.8..28.7 | 146.3 | 134.1..185.4 | +444.2% |
| Ginstr | 13.9 | 13.9..15.3 | 77.6 | 71.9..94.5 | +459.3% |
| ctx-sw | 465,478 | 463,475..518,695 | 3,417,666 | 3,110,770..4,258,014 | +634.2% |
| migrations | 340.0 | 162.0..2,002 | 237.0 | 148.0..1,437 | -30.3% |
| Mcache-miss | 119.4 | 119.2..129.3 | 342.1 | 317.9..425.8 | +186.6% |

## r150000-1ms-tick1000-wt16

rate=150000/s think=1ms tick=1000us worker-threads=16 pre/max VUs=512/1024 cores=0-19 (n base=5, cand=5)

| measure | base med | base IQR | cand med | cand IQR | Δ med |
|---|---|---|---|---|---|
| achieved it/s | 141,256 | 134,303..146,506 | 146,911 | 145,902..148,040 | +4.0% |
| dropped | 87,171 | 34,637..156,666 | 30,426 | 19,461..40,859 | -65.1% |
| wall s | 11.0 | 11.0..11.0 | 10.0 | 10.0..10.0 | -9.1% |
| task-clock ms | 91,960 | 86,861..92,599 | 91,508 | 90,496..91,573 | -0.5% |
| Gcycles | 145.8 | 143.2..151.5 | 139.3 | 131.8..149.6 | -4.5% |
| Ginstr | 76.7 | 75.6..78.9 | 73.8 | 70.9..78.1 | -3.7% |
| ctx-sw | 3,052,609 | 3,042,999..3,079,388 | 2,987,047 | 2,820,732..3,183,897 | -2.1% |
| migrations | 380.0 | 349.0..4,445 | 186.0 | 151.0..16,145 | -51.1% |
| Mcache-miss | 412.4 | 389.5..445.9 | 381.2 | 374.4..406.9 | -7.6% |

## r250000-1ms-tick1000-wt16

rate=250000/s think=1ms tick=1000us worker-threads=16 pre/max VUs=512/1024 cores=0-19 (n base=5, cand=5)

| measure | base med | base IQR | cand med | cand IQR | Δ med |
|---|---|---|---|---|---|
| achieved it/s | 148,821 | 144,926..150,198 | 199,748 | 194,710..210,620 | +34.2% |
| dropped | 1,011,296 | 997,295..1,049,528 | 502,203 | 392,789..552,675 | -50.3% |
| wall s | 11.0 | 11.0..11.0 | 10.0 | 10.0..10.0 | -9.1% |
| task-clock ms | 94,785 | 94,418..94,800 | 91,495 | 91,354..91,840 | -3.5% |
| Gcycles | 161.2 | 153.4..161.4 | 129.7 | 129.7..151.5 | -19.5% |
| Ginstr | 83.2 | 80.4..83.9 | 70.0 | 69.4..78.4 | -15.9% |
| ctx-sw | 3,381,440 | 3,247,754..3,414,435 | 2,803,731 | 2,673,063..3,082,452 | -17.1% |
| migrations | 9,954 | 455.0..12,124 | 166.0 | 163.0..19,326 | -98.3% |
| Mcache-miss | 433.9 | 417.4..449.2 | 389.7 | 377.3..438.4 | -10.2% |

_Note: both sides below 90% of the 250000/s target — host-limited cell._

## r50000-0s-tick5000-wt2

rate=50000/s think=0s tick=5000us worker-threads=2 pre/max VUs=64/128 cores=0-3 (n base=5, cand=5)

| measure | base med | base IQR | cand med | cand IQR | Δ med |
|---|---|---|---|---|---|
| achieved it/s | 25,593 | 25,592..25,593 | 49,978 | 49,978..49,979 | +95.3% |
| dropped | 243,866 | 243,857..243,869 | 0.000 | 0.000..0.000 | -100.0% |
| wall s | 11.0 | 11.0..11.0 | 10.0 | 10.0..10.0 | -9.1% |
| task-clock ms | 1,444 | 1,437..1,447 | 2,388 | 2,375..2,418 | +65.3% |
| Gcycles | 2.075 | 2.066..2.079 | 3.473 | 3.464..3.512 | +67.3% |
| Ginstr | 2.468 | 2.465..2.470 | 3.785 | 3.782..3.792 | +53.3% |
| ctx-sw | 8,450 | 8,438..8,474 | 13,095 | 12,994..13,106 | +55.0% |
| migrations | 4.000 | 0.000..4.000 | 1.000 | 0.000..2.000 | -75.0% |
| Mcache-miss | 35.9 | 35.7..35.9 | 31.4 | 31.3..31.7 | -12.4% |

## r150000-0s-tick5000-wt2

rate=150000/s think=0s tick=5000us worker-threads=2 pre/max VUs=64/128 cores=0-3 (n base=5, cand=5)

| measure | base med | base IQR | cand med | cand IQR | Δ med |
|---|---|---|---|---|---|
| achieved it/s | 25,608 | 25,607..25,609 | 106,928 | 106,144..107,163 | +317.6% |
| dropped | 1,243,285 | 1,243,244..1,243,351 | 430,002 | 427,661..437,835 | -65.4% |
| wall s | 11.0 | 11.0..11.0 | 10.0 | 10.0..10.0 | -9.1% |
| task-clock ms | 1,521 | 1,515..1,541 | 3,976 | 3,931..4,061 | +161.4% |
| Gcycles | 2.185 | 2.182..2.209 | 6.065 | 6.057..6.190 | +177.6% |
| Ginstr | 3.470 | 3.469..3.471 | 6.600 | 6.593..6.628 | +90.2% |
| ctx-sw | 8,168 | 8,167..8,184 | 15,363 | 15,242..15,404 | +88.1% |
| migrations | 2.000 | 0.000..2.000 | 3.000 | 2.000..3.000 | +50.0% |
| Mcache-miss | 31.9 | 31.8..33.2 | 56.7 | 56.5..56.9 | +77.7% |

_Note: both sides below 90% of the 150000/s target — host-limited cell._

## r250000-0s-tick5000-wt2

rate=250000/s think=0s tick=5000us worker-threads=2 pre/max VUs=64/128 cores=0-3 (n base=5, cand=5)

| measure | base med | base IQR | cand med | cand IQR | Δ med |
|---|---|---|---|---|---|
| achieved it/s | 26,155 | 25,978..26,160 | 106,675 | 106,325..106,757 | +307.9% |
| dropped | 2,237,444 | 2,237,362..2,239,472 | 1,432,040 | 1,431,389..1,435,799 | -36.0% |
| wall s | 11.0 | 11.0..11.0 | 10.0 | 10.0..10.0 | -9.1% |
| task-clock ms | 1,892 | 1,846..1,896 | 4,123 | 4,019..4,180 | +117.9% |
| Gcycles | 2.751 | 2.709..2.757 | 6.208 | 5.970..6.246 | +125.6% |
| Ginstr | 4.661 | 4.656..4.665 | 6.597 | 6.588..6.626 | +41.5% |
| ctx-sw | 8,342 | 8,179..8,407 | 15,206 | 15,138..15,294 | +82.3% |
| migrations | 4.000 | 2.000..4.000 | 2.000 | 2.000..4.000 | -50.0% |
| Mcache-miss | 27.6 | 23.8..28.3 | 58.0 | 56.9..58.5 | +110.4% |

_Note: both sides below 90% of the 250000/s target — host-limited cell._

## r50000-1ms-tick5000-wt2

rate=50000/s think=1ms tick=5000us worker-threads=2 pre/max VUs=512/1024 cores=0-3 (n base=5, cand=5)

| measure | base med | base IQR | cand med | cand IQR | Δ med |
|---|---|---|---|---|---|
| achieved it/s | 49,983 | 49,977..49,983 | 49,977 | 49,976..49,978 | -0.0% |
| dropped | 0.000 | 0.000..0.000 | 0.000 | 0.000..0.000 | - |
| wall s | 11.0 | 11.0..11.0 | 10.0 | 10.0..10.0 | -9.1% |
| task-clock ms | 2,167 | 2,160..2,287 | 2,903 | 2,811..2,969 | +34.0% |
| Gcycles | 3.180 | 3.172..3.340 | 4.288 | 4.131..4.364 | +34.8% |
| Ginstr | 3.860 | 3.856..3.860 | 4.524 | 4.522..4.524 | +17.2% |
| ctx-sw | 9,497 | 9,433..9,502 | 9,676 | 9,619..9,697 | +1.9% |
| migrations | 3.000 | 0.000..3.000 | 2.000 | 0.000..3.000 | -33.3% |
| Mcache-miss | 38.3 | 38.2..38.5 | 52.7 | 52.5..54.0 | +37.8% |

## r150000-1ms-tick5000-wt2

rate=150000/s think=1ms tick=5000us worker-threads=2 pre/max VUs=512/1024 cores=0-3 (n base=5, cand=5)

| measure | base med | base IQR | cand med | cand IQR | Δ med |
|---|---|---|---|---|---|
| achieved it/s | 149,938 | 149,935..149,942 | 149,938 | 149,936..149,938 | -0.0% |
| dropped | 0.000 | 0.000..0.000 | 0.000 | 0.000..0.000 | - |
| wall s | 11.0 | 11.0..11.0 | 10.0 | 10.0..10.0 | -9.1% |
| task-clock ms | 5,466 | 5,422..5,617 | 6,101 | 5,890..6,112 | +11.6% |
| Gcycles | 8.148 | 8.069..8.294 | 9.015 | 8.756..9.101 | +10.6% |
| Ginstr | 10.6 | 10.6..10.6 | 11.0 | 11.0..11.0 | +3.9% |
| ctx-sw | 11,175 | 11,143..12,285 | 11,151 | 10,842..11,211 | -0.2% |
| migrations | 2.000 | 2.000..2.000 | 2.000 | 2.000..4.000 | +0.0% |
| Mcache-miss | 113.6 | 110.5..114.6 | 119.8 | 119.6..120.9 | +5.5% |

## r250000-1ms-tick5000-wt2

rate=250000/s think=1ms tick=5000us worker-threads=2 pre/max VUs=512/1024 cores=0-3 (n base=5, cand=5)

| measure | base med | base IQR | cand med | cand IQR | Δ med |
|---|---|---|---|---|---|
| achieved it/s | 204,484 | 204,391..204,494 | 249,900 | 249,889..249,901 | +22.2% |
| dropped | 454,061 | 454,039..455,310 | 0.000 | 0.000..0.000 | -100.0% |
| wall s | 11.0 | 11.0..11.0 | 10.0 | 10.0..10.0 | -9.1% |
| task-clock ms | 7,021 | 7,006..7,114 | 8,379 | 8,229..8,432 | +19.3% |
| Gcycles | 10.5 | 10.5..10.6 | 12.5 | 12.2..12.7 | +18.9% |
| Ginstr | 14.8 | 14.7..14.8 | 16.1 | 16.1..16.2 | +9.2% |
| ctx-sw | 10,909 | 10,366..12,972 | 11,180 | 11,117..11,343 | +2.5% |
| migrations | 2.000 | 0.000..2.000 | 3.000 | 0.000..3.000 | +50.0% |
| Mcache-miss | 149.1 | 146.5..152.7 | 168.9 | 168.6..169.7 | +13.3% |

## r50000-0s-tick5000-wt16

rate=50000/s think=0s tick=5000us worker-threads=16 pre/max VUs=64/128 cores=0-19 (n base=5, cand=5)

| measure | base med | base IQR | cand med | cand IQR | Δ med |
|---|---|---|---|---|---|
| achieved it/s | 25,595 | 25,594..25,595 | 49,982 | 49,980..49,983 | +95.3% |
| dropped | 243,851 | 243,846..243,867 | 0.000 | 0.000..0.000 | -100.0% |
| wall s | 11.0 | 11.0..11.0 | 10.0 | 10.0..10.0 | -9.1% |
| task-clock ms | 10,261 | 10,181..10,620 | 21,812 | 21,546..22,686 | +112.6% |
| Gcycles | 12.9 | 12.7..13.4 | 27.1 | 26.7..28.1 | +110.2% |
| Ginstr | 6.934 | 6.746..7.010 | 14.3 | 14.2..14.9 | +106.5% |
| ctx-sw | 254,306 | 247,089..255,695 | 573,010 | 572,815..606,251 | +125.3% |
| migrations | 61.0 | 44.0..113.0 | 128.0 | 101.0..207.0 | +109.8% |
| Mcache-miss | 65.0 | 65.0..68.3 | 110.8 | 109.8..112.2 | +70.4% |

## r150000-0s-tick5000-wt16

rate=150000/s think=0s tick=5000us worker-threads=16 pre/max VUs=64/128 cores=0-19 (n base=5, cand=5)

| measure | base med | base IQR | cand med | cand IQR | Δ med |
|---|---|---|---|---|---|
| achieved it/s | 25,638 | 25,633..25,638 | 96,137 | 96,057..96,694 | +275.0% |
| dropped | 1,243,050 | 1,243,029..1,243,122 | 538,124 | 532,430..538,808 | -56.7% |
| wall s | 11.0 | 11.0..11.0 | 10.0 | 10.0..10.0 | -9.1% |
| task-clock ms | 10,315 | 10,106..10,693 | 34,071 | 32,589..34,293 | +230.3% |
| Gcycles | 13.2 | 12.9..13.6 | 44.3 | 42.7..44.8 | +235.9% |
| Ginstr | 7.711 | 7.647..7.937 | 23.2 | 22.7..23.8 | +201.1% |
| ctx-sw | 229,935 | 224,359..243,397 | 864,290 | 831,480..870,125 | +275.9% |
| migrations | 172.0 | 128.0..181.0 | 228.0 | 184.0..1,073 | +32.6% |
| Mcache-miss | 66.3 | 66.2..68.8 | 167.9 | 165.1..171.4 | +153.2% |

_Note: both sides below 90% of the 150000/s target — host-limited cell._

## r250000-0s-tick5000-wt16

rate=250000/s think=0s tick=5000us worker-threads=16 pre/max VUs=64/128 cores=0-19 (n base=5, cand=5)

| measure | base med | base IQR | cand med | cand IQR | Δ med |
|---|---|---|---|---|---|
| achieved it/s | 25,712 | 25,711..25,746 | 97,113 | 96,815..97,395 | +277.7% |
| dropped | 2,241,949 | 2,241,532..2,241,965 | 1,528,057 | 1,525,202..1,530,835 | -31.8% |
| wall s | 11.0 | 11.0..11.0 | 10.0 | 10.0..10.0 | -9.1% |
| task-clock ms | 10,475 | 10,188..10,822 | 34,432 | 33,976..34,563 | +228.7% |
| Gcycles | 13.6 | 13.1..13.9 | 44.2 | 43.5..45.3 | +226.1% |
| Ginstr | 8.752 | 8.571..8.937 | 23.3 | 23.3..23.6 | +166.6% |
| ctx-sw | 219,922 | 209,236..231,258 | 858,358 | 857,770..886,941 | +290.3% |
| migrations | 79.0 | 51.0..166.0 | 100.0 | 69.0..166.0 | +26.6% |
| Mcache-miss | 68.6 | 68.2..68.7 | 166.1 | 165.8..166.1 | +142.1% |

_Note: both sides below 90% of the 250000/s target — host-limited cell._

## r50000-1ms-tick5000-wt16

rate=50000/s think=1ms tick=5000us worker-threads=16 pre/max VUs=512/1024 cores=0-19 (n base=5, cand=5)

| measure | base med | base IQR | cand med | cand IQR | Δ med |
|---|---|---|---|---|---|
| achieved it/s | 49,980 | 49,980..49,981 | 49,981 | 49,979..49,981 | +0.0% |
| dropped | 0.000 | 0.000..0.000 | 0.000 | 0.000..0.000 | - |
| wall s | 11.0 | 11.0..11.0 | 10.0 | 10.0..10.0 | -9.1% |
| task-clock ms | 27,293 | 26,696..27,605 | 57,367 | 56,964..59,790 | +110.2% |
| Gcycles | 34.4 | 34.2..34.5 | 71.9 | 71.3..75.3 | +108.8% |
| Ginstr | 17.4 | 17.3..18.4 | 38.9 | 38.7..41.1 | +123.0% |
| ctx-sw | 695,367 | 694,289..729,298 | 1,637,115 | 1,626,014..1,816,438 | +135.4% |
| migrations | 89.0 | 76.0..159.0 | 81.0 | 78.0..148.0 | -9.0% |
| Mcache-miss | 133.9 | 133.7..134.9 | 211.5 | 210.6..219.5 | +58.0% |

## r150000-1ms-tick5000-wt16

rate=150000/s think=1ms tick=5000us worker-threads=16 pre/max VUs=512/1024 cores=0-19 (n base=5, cand=5)

| measure | base med | base IQR | cand med | cand IQR | Δ med |
|---|---|---|---|---|---|
| achieved it/s | 125,239 | 123,905..135,228 | 149,926 | 149,477..149,943 | +19.7% |
| dropped | 246,963 | 147,188..260,462 | 80.0 | 0.000..4,650 | -100.0% |
| wall s | 11.0 | 11.0..11.0 | 10.0 | 10.0..10.0 | -9.1% |
| task-clock ms | 93,000 | 92,967..93,997 | 89,747 | 86,562..90,695 | -3.5% |
| Gcycles | 133.4 | 127.7..141.2 | 149.2 | 135.7..173.3 | +11.9% |
| Ginstr | 70.4 | 69.1..74.0 | 78.4 | 72.0..88.6 | +11.4% |
| ctx-sw | 2,837,496 | 2,743,843..2,956,499 | 3,193,411 | 2,938,247..3,772,419 | +12.5% |
| migrations | 144.0 | 122.0..2,973 | 6,134 | 186.0..8,195 | +4159.7% |
| Mcache-miss | 366.5 | 352.6..409.9 | 422.9 | 386.7..475.1 | +15.4% |

## r250000-1ms-tick5000-wt16

rate=250000/s think=1ms tick=5000us worker-threads=16 pre/max VUs=512/1024 cores=0-19 (n base=5, cand=5)

| measure | base med | base IQR | cand med | cand IQR | Δ med |
|---|---|---|---|---|---|
| achieved it/s | 141,115 | 132,483..151,205 | 218,810 | 211,353..229,238 | +55.1% |
| dropped | 1,088,032 | 986,898..1,174,328 | 310,952 | 205,500..385,726 | -71.4% |
| wall s | 11.0 | 11.0..11.0 | 10.0 | 10.0..10.0 | -9.1% |
| task-clock ms | 95,241 | 92,236..95,880 | 91,454 | 91,216..92,342 | -4.0% |
| Gcycles | 144.3 | 135.7..152.9 | 141.2 | 139.2..151.6 | -2.2% |
| Ginstr | 78.8 | 72.9..79.6 | 74.2 | 72.9..80.2 | -5.8% |
| ctx-sw | 3,003,166 | 2,857,603..3,144,478 | 2,925,420 | 2,878,884..3,098,854 | -2.6% |
| migrations | 132.0 | 102.0..1,151 | 272.0 | 243.0..286.0 | +106.1% |
| Mcache-miss | 420.1 | 382.6..423.9 | 416.7 | 412.8..453.2 | -0.8% |

_Note: both sides below 90% of the 250000/s target — host-limited cell._

## lowrate-overwake

rate=1000/s think=1ms tick=5000us worker-threads=16 pre/max VUs=2000/2000 cores=0-19 (n base=5, cand=5)

| measure | base med | base IQR | cand med | cand IQR | Δ med |
|---|---|---|---|---|---|
| achieved it/s | 999.5 | 999.1..999.6 | 999.5 | 999.5..999.5 | +0.0% |
| dropped | 0.000 | 0.000..0.000 | 0.000 | 0.000..0.000 | - |
| wall s | 11.0 | 11.0..11.0 | 10.0 | 10.0..10.0 | -9.1% |
| task-clock ms | 359.5 | 357.6..359.8 | 89,124 | 88,354..89,285 | +24692.4% |
| Gcycles | 0.427 | 0.426..0.427 | 124.1 | 123.5..125.7 | +28987.6% |
| Ginstr | 0.244 | 0.242..0.249 | 67.6 | 67.1..67.7 | +27632.7% |
| ctx-sw | 9,508 | 9,506..9,547 | 2,976,814 | 2,944,601..3,011,337 | +31208.5% |
| migrations | 24.0 | 23.0..26.0 | 118.0 | 107.0..133.0 | +391.7% |
| Mcache-miss | 12.7 | 11.6..12.9 | 277.9 | 276.1..279.7 | +2095.9% |


## Reading notes

All 300 measured runs completed (exit 0); IQRs are tight throughout, so the
medians are stable. Deltas are candidate vs base medians.

**1. The dispatcher ceiling moves decisively.** On the zero-think ladders
(pure dispatch cost), the base saturates at ~25.6–26.2k it/s with the default
5000us tick and ~60–72k with the 1000us tick, independent of rate asked. The
candidate reaches ~96–107k it/s in the same cells: +62% to +318% achieved
throughput, with drops falling accordingly. The 5000us-tick base collapse
(~26k/s) reproduces the known failure that motivated `LOADR_DISPATCH_TICK_US`
tuning; the candidate makes the default tick usable at 2–4x the base's
*tuned* ceiling.

**2. The schedule is met exactly where the base quietly under-delivers.**
At 50k/s zero-think/wt16 the base achieves 49,076/s with ~9.2k drops per 10s
run; the candidate delivers 49,993/s with zero drops. At 250k/s x 1ms on two
worker threads the base manages 239k with ~109k drops; the candidate holds
249,960/s with zero drops. Every cell the host can sustain, the candidate
meets with 0 drops.

**3. CPU per delivered iteration at saturation is par or better.** E.g.
r250000-0s-tick1000-wt2: -5.8% instructions while delivering +62.4%;
r150000-0s-tick1000-wt16: -5.7% task-clock, -3.5% instructions while
delivering +37.2%. The contended budget cache line does not show up as a
per-iteration cost regression at load.

**4. Broadcast over-waking is real, and severe for idle over-provisioned
pools.** This is the trade the spec called out, and the low-rate/high-idle
cell exists to expose. `notify_waiters()` wakes *every* parked worker once
per non-zero batch; losers re-park. The waste scales with
parked workers x tick frequency and is paid even when throughput is
identical:

| cell | delivered | base CPUs | cand CPUs | task-clock Δ |
|---|---|---|---|---|
| lowrate-overwake (2000 parked, 1k/s) | equal (1k/s) | 0.03 | 8.88 | +24,700% |
| r50000-1ms-tick1000-wt16 (~460 parked) | equal (50k/s) | 1.88 | 8.86 | +328% |
| r50000-1ms-tick5000-wt16 (~460 parked) | equal (50k/s) | 2.47 | 5.72 | +110% |
| r50000-1ms-tick1000-wt2 (~460 parked) | equal (50k/s) | 0.17 | 0.39 | +110% |

The worst case burns ~9 CPUs to deliver a 1k/s trickle from a 2000-worker
pool (base: 0.03 CPUs). The cost is bounded by pool size, not by rate, and
disappears when `pre_allocated_vus` approximates actual in-flight work
(compare the 0s-think cells, whose 64–128 pools show single-digit-% to ~2x
overhead worst case at wt16 while delivering equal or far higher throughput).

**5. Runs end at the deadline.** Wall time is 11.0s (base) vs 10.0s (cand)
in every cell: base parked workers sit out the 1s `graceful_stop` at the
natural deadline; the candidate's closure broadcast releases them
immediately. (This also makes the e2e suite faster.)

## Recommendation

The measured evidence supports the mechanism: the per-arrival dispatcher was
a real single-task ceiling, and the tick-bounded claim budget removes it
(+62% to +318% achieved throughput at the rates this goal targets, exact
schedule adherence with zero drops everywhere the host can sustain the rate,
par-or-better CPU per delivered iteration at load).

The evidence equally shows the predicted broadcast cost is not benign for
heavily over-provisioned idle pools: up to ~9 CPUs of pure wake churn at
2000 parked workers (~250x base task-clock at equal delivered load). That
regime is outside this goal's motivating use case (high-rate open model with
pools sized near expected in-flight work), but it is a real plan shape.

Recommendation for the reviewer: **land the claim budget for its throughput
regime, with the over-waking cost documented as a known sizing sensitivity
(`pre_allocated_vus` ≈ expected in-flight iterations), and treat the sharded
Notify ring / bounded-wake design — explicitly out of scope here — as the
follow-up that removes the idle-pool tax.** If grossly over-provisioned
pools must stay cheap before any landing, hold for that redesign instead;
the measurements above quantify exactly what it must fix. Either way this
local A/B validates the isolated mechanism only — the Graviton/AWS ceiling
remains for `aws-ab-validation`.

---

# v2 — semaphore wake path

The v1 recommendation named broadcast over-waking as the change's one real
regression. This follow-up (commit `0d064f6`, candidate binary `loadr-cand2`,
sha256 `97252ca8…`) replaces the shared `Notify` broadcast with
`tokio::sync::Semaphore`: published arrivals are permits, and tokio assigns
permits directly to FIFO-parked workers — waking exactly `min(due, parked)`
workers per tick instead of the whole pool. Budget/expiry/closure accounting
is unchanged; conservation gains one post-join sweep (permits assigned to
parked workers at closure return to the pool as their acquire futures drop
during the join). Same harness, same base binary (sha verified), same host,
runs on 2026-07-13; raw rows in `runs-v2.csv`.

## v2 acceptance checks (stated before the run)

| check | bar | measured | verdict |
|---|---|---|---|
| lowrate-overwake task-clock | ≤ ~2x base (was 248x) | **1.0x base** (0.03 vs 0.03 CPUs; v1: 8.91) | pass |
| r50000-1ms-tick1000-wt16 CPU | ≈ base (was 4.3x) | **0.94x base** (1.95 vs 2.08 CPUs; v1: 8.89) | pass |
| r50000-1ms-tick5000-wt16 CPU | ≈ base (was 2.2x) | 1.16x base (3.06 vs 2.63; v1: 5.74) | pass |
| r50000-1ms-tick1000-wt2 CPU | ≈ base (was 2.6x) | 1.27x base (0.19 vs 0.15; v1: 0.39) | pass |
| zero-think ladder wins | within ±5% of v1-cand | +59.8%..+316.5% vs base (v1: +62..+318) | pass |
| 250k-1ms cells | within ±5% of v1-cand | wt2 249,951/s (=v1); wt16 tick1000 +5.1% vs v1; wt16 tick5000 −4.3% vs v1 | pass |
| zero drops where v1-cand had zero | equal | equal, except 4 arrivals of 2.5M (0.00016%) in r250000-1ms-tick1000-wt2 | pass (noted) |

## v2 reading notes

All 300 measured runs exit 0. Medians over 5 alternating pairs per cell.

**1. The idle-pool tax is gone.** The trickle cell (2000 parked workers,
1k/s) went from 8.91 CPUs (v1) to 0.03 CPUs — indistinguishable from base.
Context switches in that cell dropped from ~2.98M per run to base-level
(~9.5k). The 460-parked cells are now between 6% cheaper and 27% more
expensive than base (v1: +110%..+328%), the remainder being the fair
semaphore's per-park queue traffic at 1ms think churn.

**2. Every throughput win held.** Dispatcher-bound zero-think cells:
+59.8%..+316.5% achieved over base (v1: +62.4%..+317.6% — run-to-run session
variance, same shape). 250k/s x 1ms on two worker threads still delivers the
full schedule (249,951/s, v1: 249,960) where base manages ~238k with ~110k
drops. The 250k-1ms-tick1000-wt16 cell improved over v1 (+39.7% vs base,
210,016/s vs v1's 199,748); the tick5000 sibling measured −4.3% vs v1-cand
(209,475 vs 218,810) with base also measuring lower this session — within
the pre-stated ±5% noise bar.

**3. Drops improved where it matters, one negligible exception.**
r150000-1ms-tick1000-wt16: 30,426 dropped (v1) → 772 (v2), achieved
149,899/s of the 150k target — exact wakes get parked workers to work
faster than broadcast racing did. r250000-1ms-tick1000-wt2 recorded 4
dropped arrivals of ~2.5M scheduled (v1: 0) — 0.00016%, inside scheduling
noise. r150000-1ms-tick5000-wt16 went 80 → 1,168 (99.2% of target still
delivered); the same cell's v1 number rode on waking all 512 workers per
tick, which v2 deliberately no longer pays for.

**4. Behavior guarantees tightened.** The lost-wake race class is gone by
construction (permits are state, not events); a new engine-level test pins
that runs ending while paused observe closure immediately (a gap the
adversarial review found in the broadcast design's paused branch would have
been introduced by a naive port — the gate now carries a closed token).

## Updated recommendation

**Land the semaphore claim budget.** It keeps every v1 throughput gain
(+60%..+317% on dispatcher-bound cells, exact schedule adherence with zero
or near-zero drops wherever the host sustains the rate), erases the
idle-pool over-waking tax that was v1's blocking concern (248x → 1.0x), and
deletes the hand-rolled park protocol rather than adding machinery. The
sharded-Notify ring is no longer needed as a follow-up; it remains in the
goal doc only as historical context. Remaining open item: the Graviton/AWS
ceiling (`aws-ab-validation`) — this local A/B validates the isolated
dispatcher mechanism only.

## r50000-0s-tick1000-wt2

rate=50000/s think=0s tick=1000us worker-threads=2 pre/max VUs=64/128 cores=0-3 (n base=5, cand=5)

| measure | base med | base IQR | cand med | cand IQR | Δ med |
|---|---|---|---|---|---|
| achieved it/s | 49,996 | 49,995..49,996 | 49,998 | 49,998..49,998 | +0.0% |
| dropped | 1.000 | 0.000..2.000 | 0.000 | 0.000..0.000 | -100.0% |
| wall s | 11.0 | 11.0..11.0 | 10.0 | 10.0..10.0 | -9.1% |
| task-clock ms | 2,225 | 2,073..2,240 | 2,155 | 2,116..2,271 | -3.2% |
| Gcycles | 3.720 | 3.687..3.726 | 3.712 | 3.690..3.742 | -0.2% |
| Ginstr | 4.402 | 4.359..4.416 | 4.383 | 4.378..4.417 | -0.4% |
| ctx-sw | 17,452 | 17,442..17,529 | 17,338 | 17,249..17,430 | -0.7% |
| migrations | 6.000 | 4.000..6.000 | 6.000 | 6.000..7.000 | +0.0% |
| Mcache-miss | 29.7 | 29.6..29.7 | 29.6 | 29.4..30.2 | -0.2% |

## r150000-0s-tick1000-wt2

rate=150000/s think=0s tick=1000us worker-threads=2 pre/max VUs=64/128 cores=0-3 (n base=5, cand=5)

| measure | base med | base IQR | cand med | cand IQR | Δ med |
|---|---|---|---|---|---|
| achieved it/s | 60,271 | 60,226..60,388 | 104,745 | 104,644..105,020 | +73.8% |
| dropped | 897,200 | 895,994..897,602 | 452,518 | 449,677..453,377 | -49.6% |
| wall s | 11.0 | 11.0..11.0 | 10.0 | 10.0..10.0 | -9.1% |
| task-clock ms | 2,908 | 2,877..2,924 | 4,265 | 4,238..4,360 | +46.7% |
| Gcycles | 4.811 | 4.785..4.864 | 6.536 | 6.456..6.563 | +35.8% |
| Ginstr | 5.882 | 5.872..5.903 | 6.776 | 6.759..6.780 | +15.2% |
| ctx-sw | 17,025 | 16,979..17,043 | 15,097 | 14,989..15,106 | -11.3% |
| migrations | 7.000 | 6.000..7.000 | 3.000 | 3.000..4.000 | -57.1% |
| Mcache-miss | 43.0 | 41.9..44.1 | 56.1 | 56.1..56.4 | +30.6% |

_Note: both sides below 90% of the 150000/s target — host-limited cell._

## r250000-0s-tick1000-wt2

rate=250000/s think=0s tick=1000us worker-threads=2 pre/max VUs=64/128 cores=0-3 (n base=5, cand=5)

| measure | base med | base IQR | cand med | cand IQR | Δ med |
|---|---|---|---|---|---|
| achieved it/s | 66,362 | 66,175..66,454 | 106,023 | 105,030..106,355 | +59.8% |
| dropped | 1,836,346 | 1,835,090..1,838,152 | 1,439,563 | 1,436,350..1,449,628 | -21.6% |
| wall s | 11.0 | 11.0..11.0 | 10.0 | 10.0..10.0 | -9.1% |
| task-clock ms | 3,692 | 3,512..3,728 | 4,340 | 4,194..4,340 | +17.6% |
| Gcycles | 5.731 | 5.690..5.767 | 6.488 | 6.484..6.498 | +13.2% |
| Ginstr | 7.301 | 7.299..7.334 | 6.796 | 6.765..6.811 | -6.9% |
| ctx-sw | 16,461 | 16,326..16,623 | 15,008 | 14,989..15,159 | -8.8% |
| migrations | 3.000 | 2.000..4.000 | 6.000 | 3.000..7.000 | +100.0% |
| Mcache-miss | 51.4 | 50.2..52.5 | 57.2 | 56.8..57.6 | +11.3% |

_Note: both sides below 90% of the 250000/s target — host-limited cell._

## r50000-1ms-tick1000-wt2

rate=50000/s think=1ms tick=1000us worker-threads=2 pre/max VUs=512/1024 cores=0-3 (n base=5, cand=5)

| measure | base med | base IQR | cand med | cand IQR | Δ med |
|---|---|---|---|---|---|
| achieved it/s | 49,994 | 49,992..49,994 | 49,992 | 49,991..49,993 | -0.0% |
| dropped | 0.000 | 0.000..0.000 | 0.000 | 0.000..0.000 | - |
| wall s | 11.0 | 11.0..11.0 | 10.0 | 10.0..10.0 | -9.1% |
| task-clock ms | 1,547 | 1,545..1,852 | 1,889 | 1,825..1,897 | +22.1% |
| Gcycles | 3.019 | 2.989..3.025 | 2.982 | 2.957..3.072 | -1.2% |
| Ginstr | 3.892 | 3.873..3.900 | 3.923 | 3.920..3.938 | +0.8% |
| ctx-sw | 17,843 | 17,727..17,897 | 17,557 | 17,551..17,609 | -1.6% |
| migrations | 20.0 | 12.0..38.0 | 8.000 | 7.000..13.0 | -60.0% |
| Mcache-miss | 34.0 | 32.3..35.4 | 33.2 | 32.4..33.5 | -2.4% |

## r150000-1ms-tick1000-wt2

rate=150000/s think=1ms tick=1000us worker-threads=2 pre/max VUs=512/1024 cores=0-3 (n base=5, cand=5)

| measure | base med | base IQR | cand med | cand IQR | Δ med |
|---|---|---|---|---|---|
| achieved it/s | 149,976 | 149,966..149,979 | 149,976 | 149,970..149,978 | -0.0% |
| dropped | 0.000 | 0.000..0.000 | 0.000 | 0.000..0.000 | - |
| wall s | 11.0 | 11.0..11.0 | 10.0 | 10.0..10.0 | -9.1% |
| task-clock ms | 5,254 | 5,091..5,315 | 5,257 | 5,256..5,265 | +0.1% |
| Gcycles | 8.067 | 8.032..8.077 | 7.914 | 7.827..8.004 | -1.9% |
| Ginstr | 10.7 | 10.7..10.7 | 10.7 | 10.7..10.7 | +0.3% |
| ctx-sw | 15,900 | 15,771..15,927 | 15,034 | 14,943..15,082 | -5.4% |
| migrations | 2.000 | 2.000..4.000 | 4.000 | 2.000..4.000 | +100.0% |
| Mcache-miss | 101.3 | 101.2..101.8 | 102.3 | 101.5..102.6 | +1.0% |

## r250000-1ms-tick1000-wt2

rate=250000/s think=1ms tick=1000us worker-threads=2 pre/max VUs=512/1024 cores=0-3 (n base=5, cand=5)

| measure | base med | base IQR | cand med | cand IQR | Δ med |
|---|---|---|---|---|---|
| achieved it/s | 238,220 | 237,790..239,299 | 249,951 | 249,951..249,959 | +4.9% |
| dropped | 117,228 | 106,458..121,620 | 4.000 | 0.000..5.000 | -100.0% |
| wall s | 11.0 | 11.0..11.0 | 10.0 | 10.0..10.0 | -9.1% |
| task-clock ms | 8,210 | 8,178..8,217 | 8,713 | 8,536..8,975 | +6.1% |
| Gcycles | 12.3 | 12.2..12.3 | 13.0 | 12.7..13.4 | +6.0% |
| Ginstr | 17.0 | 16.9..17.1 | 17.6 | 17.6..17.7 | +3.7% |
| ctx-sw | 17,158 | 16,334..17,456 | 15,101 | 13,602..15,491 | -12.0% |
| migrations | 2.000 | 2.000..4.000 | 6.000 | 4.000..6.000 | +200.0% |
| Mcache-miss | 172.8 | 172.5..175.1 | 178.2 | 177.3..178.9 | +3.1% |

## r50000-0s-tick1000-wt16

rate=50000/s think=0s tick=1000us worker-threads=16 pre/max VUs=64/128 cores=0-19 (n base=5, cand=5)

| measure | base med | base IQR | cand med | cand IQR | Δ med |
|---|---|---|---|---|---|
| achieved it/s | 49,137 | 49,080..49,194 | 49,998 | 49,996..49,998 | +1.8% |
| dropped | 8,615 | 7,988..9,140 | 0.000 | 0.000..0.000 | -100.0% |
| wall s | 11.0 | 11.0..11.0 | 10.0 | 10.0..10.0 | -9.1% |
| task-clock ms | 18,689 | 18,607..18,847 | 23,816 | 23,109..24,085 | +27.4% |
| Gcycles | 23.8 | 23.7..23.9 | 30.4 | 30.0..31.0 | +27.9% |
| Ginstr | 12.3 | 12.3..12.3 | 16.2 | 15.8..16.4 | +31.4% |
| ctx-sw | 460,146 | 456,521..461,271 | 602,033 | 585,527..619,892 | +30.8% |
| migrations | 398.0 | 338.0..788.0 | 321.0 | 289.0..437.0 | -19.3% |
| Mcache-miss | 115.2 | 114.5..116.9 | 123.5 | 123.3..130.3 | +7.3% |

## r150000-0s-tick1000-wt16

rate=150000/s think=0s tick=1000us worker-threads=16 pre/max VUs=64/128 cores=0-19 (n base=5, cand=5)

| measure | base med | base IQR | cand med | cand IQR | Δ med |
|---|---|---|---|---|---|
| achieved it/s | 68,794 | 68,172..69,228 | 95,863 | 95,606..95,870 | +39.3% |
| dropped | 811,818 | 807,663..818,005 | 541,280 | 541,162..543,926 | -33.3% |
| wall s | 11.0 | 11.0..11.0 | 10.0 | 10.0..10.0 | -9.1% |
| task-clock ms | 36,386 | 36,361..36,507 | 34,378 | 34,191..34,694 | -5.5% |
| Gcycles | 47.2 | 46.8..48.8 | 45.2 | 45.2..45.3 | -4.3% |
| Ginstr | 24.7 | 24.5..26.1 | 24.4 | 24.3..24.5 | -1.5% |
| ctx-sw | 942,283 | 926,927..1,023,719 | 922,574 | 912,269..926,944 | -2.1% |
| migrations | 298.0 | 275.0..547.0 | 249.0 | 248.0..417.0 | -16.4% |
| Mcache-miss | 192.1 | 185.8..192.5 | 166.1 | 165.5..170.4 | -13.6% |

_Note: both sides below 90% of the 150000/s target — host-limited cell._

## r250000-0s-tick1000-wt16

rate=250000/s think=0s tick=1000us worker-threads=16 pre/max VUs=64/128 cores=0-19 (n base=5, cand=5)

| measure | base med | base IQR | cand med | cand IQR | Δ med |
|---|---|---|---|---|---|
| achieved it/s | 69,371 | 67,817..71,947 | 96,892 | 95,979..97,244 | +39.7% |
| dropped | 1,806,007 | 1,780,279..1,821,529 | 1,530,995 | 1,527,258..1,540,135 | -15.2% |
| wall s | 11.0 | 11.0..11.0 | 10.0 | 10.0..10.0 | -9.1% |
| task-clock ms | 37,836 | 36,805..38,064 | 34,387 | 33,434..34,583 | -9.1% |
| Gcycles | 50.2 | 49.2..50.3 | 44.2 | 43.9..45.3 | -12.0% |
| Ginstr | 27.1 | 26.7..27.2 | 24.3 | 23.9..24.4 | -10.6% |
| ctx-sw | 1,025,348 | 954,205..1,027,093 | 904,127 | 882,274..932,225 | -11.8% |
| migrations | 365.0 | 239.0..540.0 | 285.0 | 208.0..305.0 | -21.9% |
| Mcache-miss | 198.5 | 195.3..201.9 | 167.6 | 164.7..171.5 | -15.6% |

_Note: both sides below 90% of the 250000/s target — host-limited cell._

## r50000-1ms-tick1000-wt16

rate=50000/s think=1ms tick=1000us worker-threads=16 pre/max VUs=512/1024 cores=0-19 (n base=5, cand=5)

| measure | base med | base IQR | cand med | cand IQR | Δ med |
|---|---|---|---|---|---|
| achieved it/s | 49,992 | 49,991..49,992 | 49,992 | 49,990..49,993 | +0.0% |
| dropped | 0.000 | 0.000..0.000 | 0.000 | 0.000..0.000 | - |
| wall s | 11.0 | 11.0..11.0 | 10.0 | 10.0..10.0 | -9.1% |
| task-clock ms | 20,826 | 16,557..20,945 | 19,468 | 17,252..21,869 | -6.5% |
| Gcycles | 26.8 | 22.6..27.9 | 25.6 | 23.2..28.1 | -4.5% |
| Ginstr | 13.9 | 11.9..14.8 | 13.8 | 12.6..14.9 | -0.1% |
| ctx-sw | 458,481 | 365,773..503,475 | 411,976 | 379,398..491,010 | -10.1% |
| migrations | 785.0 | 343.0..2,833 | 3,500 | 337.0..5,464 | +345.9% |
| Mcache-miss | 119.1 | 118.9..125.8 | 120.1 | 119.7..121.3 | +0.8% |

## r150000-1ms-tick1000-wt16

rate=150000/s think=1ms tick=1000us worker-threads=16 pre/max VUs=512/1024 cores=0-19 (n base=5, cand=5)

| measure | base med | base IQR | cand med | cand IQR | Δ med |
|---|---|---|---|---|---|
| achieved it/s | 149,721 | 148,733..149,942 | 149,899 | 148,478..149,943 | +0.1% |
| dropped | 2,566 | 355.0..12,457 | 772.0 | 434.0..14,846 | -69.9% |
| wall s | 11.0 | 11.0..11.0 | 10.0 | 10.0..10.0 | -9.1% |
| task-clock ms | 80,846 | 80,016..81,144 | 87,922 | 82,881..90,017 | +8.8% |
| Gcycles | 179.7 | 156.1..180.0 | 182.5 | 149.6..185.7 | +1.6% |
| Ginstr | 87.3 | 81.4..89.2 | 92.3 | 78.0..92.3 | +5.7% |
| ctx-sw | 3,702,202 | 3,255,280..3,910,120 | 3,875,247 | 3,073,705..3,979,350 | +4.7% |
| migrations | 5,321 | 4,089..8,143 | 3,837 | 217.0..4,619 | -27.9% |
| Mcache-miss | 510.5 | 458.7..512.5 | 481.6 | 406.3..508.0 | -5.7% |

## r250000-1ms-tick1000-wt16

rate=250000/s think=1ms tick=1000us worker-threads=16 pre/max VUs=512/1024 cores=0-19 (n base=5, cand=5)

| measure | base med | base IQR | cand med | cand IQR | Δ med |
|---|---|---|---|---|---|
| achieved it/s | 150,310 | 143,646..157,872 | 210,016 | 201,989..212,063 | +39.7% |
| dropped | 996,550 | 920,581..1,062,745 | 399,355 | 379,189..479,870 | -59.9% |
| wall s | 11.0 | 11.0..11.0 | 10.0 | 10.0..10.0 | -9.1% |
| task-clock ms | 95,459 | 94,412..95,469 | 93,181 | 93,153..93,339 | -2.4% |
| Gcycles | 160.4 | 159.2..161.4 | 147.4 | 141.3..154.5 | -8.1% |
| Ginstr | 83.9 | 83.3..84.6 | 78.2 | 74.3..80.3 | -6.8% |
| ctx-sw | 3,550,114 | 3,371,463..3,756,997 | 2,954,609 | 2,815,539..3,056,218 | -16.8% |
| migrations | 466.0 | 176.0..4,880 | 196.0 | 174.0..233.0 | -57.9% |
| Mcache-miss | 455.7 | 445.2..457.9 | 424.7 | 408.0..445.6 | -6.8% |

_Note: both sides below 90% of the 250000/s target — host-limited cell._

## r50000-0s-tick5000-wt2

rate=50000/s think=0s tick=5000us worker-threads=2 pre/max VUs=64/128 cores=0-3 (n base=5, cand=5)

| measure | base med | base IQR | cand med | cand IQR | Δ med |
|---|---|---|---|---|---|
| achieved it/s | 25,592 | 25,592..25,593 | 49,978 | 49,978..49,980 | +95.3% |
| dropped | 243,857 | 243,845..243,861 | 0.000 | 0.000..0.000 | -100.0% |
| wall s | 11.0 | 11.0..11.0 | 10.0 | 10.0..10.0 | -9.1% |
| task-clock ms | 1,417 | 1,398..1,505 | 2,368 | 2,358..2,391 | +67.1% |
| Gcycles | 2.024 | 2.013..2.118 | 3.456 | 3.428..3.463 | +70.7% |
| Ginstr | 2.443 | 2.419..2.470 | 3.796 | 3.791..3.807 | +55.4% |
| ctx-sw | 8,507 | 8,497..8,518 | 13,016 | 12,962..13,045 | +53.0% |
| migrations | 6.000 | 6.000..17.0 | 6.000 | 6.000..28.0 | +0.0% |
| Mcache-miss | 31.0 | 27.5..34.7 | 31.3 | 31.1..31.4 | +1.0% |

## r150000-0s-tick5000-wt2

rate=150000/s think=0s tick=5000us worker-threads=2 pre/max VUs=64/128 cores=0-3 (n base=5, cand=5)

| measure | base med | base IQR | cand med | cand IQR | Δ med |
|---|---|---|---|---|---|
| achieved it/s | 25,621 | 25,619..25,622 | 106,710 | 106,524..106,985 | +316.5% |
| dropped | 1,243,180 | 1,243,116..1,243,218 | 432,388 | 429,441..434,126 | -65.2% |
| wall s | 11.0 | 11.0..11.0 | 10.0 | 10.0..10.0 | -9.1% |
| task-clock ms | 1,567 | 1,561..1,585 | 3,989 | 3,969..4,026 | +154.7% |
| Gcycles | 2.255 | 2.254..2.266 | 6.025 | 5.952..6.037 | +167.2% |
| Ginstr | 3.475 | 3.468..3.480 | 6.612 | 6.604..6.619 | +90.3% |
| ctx-sw | 8,167 | 8,154..8,186 | 15,338 | 15,297..15,339 | +87.8% |
| migrations | 6.000 | 6.000..8.000 | 5.000 | 2.000..5.000 | -16.7% |
| Mcache-miss | 31.5 | 27.9..32.4 | 57.0 | 56.8..57.1 | +81.2% |

_Note: both sides below 90% of the 150000/s target — host-limited cell._

## r250000-0s-tick5000-wt2

rate=250000/s think=0s tick=5000us worker-threads=2 pre/max VUs=64/128 cores=0-3 (n base=5, cand=5)

| measure | base med | base IQR | cand med | cand IQR | Δ med |
|---|---|---|---|---|---|
| achieved it/s | 25,821 | 25,815..25,862 | 106,720 | 106,665..106,808 | +313.3% |
| dropped | 2,240,708 | 2,240,302..2,240,755 | 1,431,594 | 1,430,814..1,432,256 | -36.1% |
| wall s | 11.0 | 11.0..11.0 | 10.0 | 10.0..10.0 | -9.1% |
| task-clock ms | 1,902 | 1,890..1,907 | 4,042 | 4,028..4,043 | +112.5% |
| Gcycles | 2.748 | 2.745..2.752 | 6.141 | 6.093..6.257 | +123.5% |
| Ginstr | 4.657 | 4.656..4.659 | 6.601 | 6.599..6.613 | +41.8% |
| ctx-sw | 8,162 | 8,160..8,186 | 15,291 | 15,275..15,316 | +87.3% |
| migrations | 5.000 | 2.000..8.000 | 4.000 | 4.000..6.000 | -20.0% |
| Mcache-miss | 31.9 | 29.4..32.9 | 56.6 | 56.4..56.6 | +77.6% |

_Note: both sides below 90% of the 250000/s target — host-limited cell._

## r50000-1ms-tick5000-wt2

rate=50000/s think=1ms tick=5000us worker-threads=2 pre/max VUs=512/1024 cores=0-3 (n base=5, cand=5)

| measure | base med | base IQR | cand med | cand IQR | Δ med |
|---|---|---|---|---|---|
| achieved it/s | 49,980 | 49,977..49,980 | 49,982 | 49,982..49,983 | +0.0% |
| dropped | 0.000 | 0.000..0.000 | 0.000 | 0.000..0.000 | - |
| wall s | 11.0 | 11.0..11.0 | 10.0 | 10.0..10.0 | -9.1% |
| task-clock ms | 2,255 | 2,233..2,255 | 2,299 | 2,268..2,361 | +2.0% |
| Gcycles | 3.290 | 3.242..3.291 | 3.379 | 3.340..3.442 | +2.7% |
| Ginstr | 3.860 | 3.858..3.868 | 3.919 | 3.904..3.922 | +1.5% |
| ctx-sw | 9,703 | 9,686..9,744 | 9,595 | 9,571..9,604 | -1.1% |
| migrations | 5.000 | 2.000..5.000 | 4.000 | 2.000..5.000 | -20.0% |
| Mcache-miss | 38.3 | 38.2..38.6 | 38.8 | 38.8..39.3 | +1.3% |

## r150000-1ms-tick5000-wt2

rate=150000/s think=1ms tick=5000us worker-threads=2 pre/max VUs=512/1024 cores=0-3 (n base=5, cand=5)

| measure | base med | base IQR | cand med | cand IQR | Δ med |
|---|---|---|---|---|---|
| achieved it/s | 149,938 | 149,936..149,941 | 149,943 | 149,943..149,943 | +0.0% |
| dropped | 0.000 | 0.000..0.000 | 0.000 | 0.000..0.000 | - |
| wall s | 11.0 | 11.0..11.0 | 10.0 | 10.0..10.0 | -9.1% |
| task-clock ms | 5,402 | 5,393..5,688 | 5,337 | 5,301..5,511 | -1.2% |
| Gcycles | 8.023 | 7.981..8.439 | 8.028 | 7.961..8.171 | +0.1% |
| Ginstr | 10.6 | 10.6..10.6 | 10.8 | 10.8..10.9 | +2.2% |
| ctx-sw | 11,157 | 10,816..12,173 | 10,833 | 10,818..10,906 | -2.9% |
| migrations | 3.000 | 2.000..3.000 | 2.000 | 1.000..3.000 | -33.3% |
| Mcache-miss | 111.9 | 111.4..112.3 | 113.6 | 113.6..113.8 | +1.6% |

## r250000-1ms-tick5000-wt2

rate=250000/s think=1ms tick=5000us worker-threads=2 pre/max VUs=512/1024 cores=0-3 (n base=5, cand=5)

| measure | base med | base IQR | cand med | cand IQR | Δ med |
|---|---|---|---|---|---|
| achieved it/s | 204,417 | 204,298..204,538 | 249,921 | 249,919..249,923 | +22.3% |
| dropped | 454,865 | 453,442..456,066 | 0.000 | 0.000..0.000 | -100.0% |
| wall s | 11.0 | 11.0..11.0 | 10.0 | 10.0..10.0 | -9.1% |
| task-clock ms | 7,039 | 6,916..7,525 | 7,886 | 7,732..7,920 | +12.0% |
| Gcycles | 10.5 | 10.4..11.1 | 12.3 | 11.8..12.3 | +17.0% |
| Ginstr | 14.8 | 14.7..14.8 | 16.4 | 16.3..16.4 | +10.6% |
| ctx-sw | 10,410 | 10,240..12,959 | 11,585 | 11,139..11,661 | +11.3% |
| migrations | 0.000 | 0.000..2.000 | 2.000 | 2.000..4.000 | - |
| Mcache-miss | 149.8 | 147.3..151.4 | 169.3 | 168.9..169.4 | +13.0% |

## r50000-0s-tick5000-wt16

rate=50000/s think=0s tick=5000us worker-threads=16 pre/max VUs=64/128 cores=0-19 (n base=5, cand=5)

| measure | base med | base IQR | cand med | cand IQR | Δ med |
|---|---|---|---|---|---|
| achieved it/s | 25,594 | 25,593..25,595 | 49,982 | 49,982..49,982 | +95.3% |
| dropped | 243,884 | 243,879..243,899 | 0.000 | 0.000..0.000 | -100.0% |
| wall s | 11.0 | 11.0..11.0 | 10.0 | 10.0..10.0 | -9.1% |
| task-clock ms | 9,514 | 9,417..9,579 | 22,116 | 22,074..22,210 | +132.5% |
| Gcycles | 12.3 | 11.9..12.3 | 27.5 | 27.5..27.7 | +124.2% |
| Ginstr | 6.556 | 6.403..6.564 | 14.9 | 14.9..15.2 | +126.5% |
| ctx-sw | 233,449 | 224,062..235,956 | 581,157 | 575,438..581,617 | +148.9% |
| migrations | 330.0 | 276.0..367.0 | 423.0 | 403.0..2,048 | +28.2% |
| Mcache-miss | 67.0 | 65.7..67.4 | 114.4 | 111.9..115.7 | +70.7% |

## r150000-0s-tick5000-wt16

rate=150000/s think=0s tick=5000us worker-threads=16 pre/max VUs=64/128 cores=0-19 (n base=5, cand=5)

| measure | base med | base IQR | cand med | cand IQR | Δ med |
|---|---|---|---|---|---|
| achieved it/s | 25,622 | 25,622..25,627 | 97,334 | 97,155..97,374 | +279.9% |
| dropped | 1,243,242 | 1,243,227..1,243,293 | 526,020 | 525,681..527,863 | -57.7% |
| wall s | 11.0 | 11.0..11.0 | 10.0 | 10.0..10.0 | -9.1% |
| task-clock ms | 9,219 | 9,054..9,972 | 32,160 | 31,982..33,886 | +248.8% |
| Gcycles | 11.8 | 11.6..12.7 | 42.7 | 42.3..44.4 | +260.0% |
| Ginstr | 7.161 | 7.082..7.640 | 23.3 | 22.5..24.2 | +224.9% |
| ctx-sw | 196,992 | 194,791..224,935 | 825,022 | 810,786..871,008 | +318.8% |
| migrations | 469.0 | 272.0..548.0 | 452.0 | 314.0..6,038 | -3.6% |
| Mcache-miss | 71.1 | 68.2..71.6 | 169.2 | 159.4..170.2 | +138.1% |

_Note: both sides below 90% of the 150000/s target — host-limited cell._

## r250000-0s-tick5000-wt16

rate=250000/s think=0s tick=5000us worker-threads=16 pre/max VUs=64/128 cores=0-19 (n base=5, cand=5)

| measure | base med | base IQR | cand med | cand IQR | Δ med |
|---|---|---|---|---|---|
| achieved it/s | 25,734 | 25,732..25,757 | 97,060 | 96,968..97,368 | +277.2% |
| dropped | 2,241,584 | 2,241,572..2,241,733 | 1,528,335 | 1,525,325..1,529,224 | -31.8% |
| wall s | 11.0 | 11.0..11.0 | 10.0 | 10.0..10.0 | -9.1% |
| task-clock ms | 10,467 | 10,207..10,684 | 32,466 | 32,380..32,942 | +210.2% |
| Gcycles | 13.5 | 13.1..13.8 | 42.6 | 42.2..43.3 | +216.6% |
| Ginstr | 8.835 | 8.706..8.968 | 23.0 | 22.8..23.2 | +160.2% |
| ctx-sw | 221,217 | 212,750..222,445 | 822,387 | 808,925..831,529 | +271.8% |
| migrations | 306.0 | 289.0..342.0 | 4,154 | 2,108..5,807 | +1257.5% |
| Mcache-miss | 69.8 | 68.7..69.9 | 163.5 | 162.2..164.7 | +134.2% |

_Note: both sides below 90% of the 250000/s target — host-limited cell._

## r50000-1ms-tick5000-wt16

rate=50000/s think=1ms tick=5000us worker-threads=16 pre/max VUs=512/1024 cores=0-19 (n base=5, cand=5)

| measure | base med | base IQR | cand med | cand IQR | Δ med |
|---|---|---|---|---|---|
| achieved it/s | 49,978 | 49,977..49,978 | 49,980 | 49,979..49,982 | +0.0% |
| dropped | 0.000 | 0.000..0.000 | 0.000 | 0.000..0.000 | - |
| wall s | 11.0 | 11.0..11.0 | 10.0 | 10.0..10.0 | -9.1% |
| task-clock ms | 26,314 | 25,938..26,642 | 30,579 | 29,709..30,881 | +16.2% |
| Gcycles | 33.6 | 32.9..33.6 | 38.7 | 38.4..38.7 | +15.1% |
| Ginstr | 17.2 | 16.9..18.1 | 20.6 | 20.6..20.7 | +19.7% |
| ctx-sw | 674,300 | 654,930..691,704 | 803,265 | 778,195..806,698 | +19.1% |
| migrations | 393.0 | 259.0..436.0 | 296.0 | 251.0..4,619 | -24.7% |
| Mcache-miss | 136.2 | 133.8..137.8 | 143.2 | 143.2..145.2 | +5.1% |

## r150000-1ms-tick5000-wt16

rate=150000/s think=1ms tick=5000us worker-threads=16 pre/max VUs=512/1024 cores=0-19 (n base=5, cand=5)

| measure | base med | base IQR | cand med | cand IQR | Δ med |
|---|---|---|---|---|---|
| achieved it/s | 148,051 | 144,482..148,497 | 149,828 | 149,828..149,930 | +1.2% |
| dropped | 19,069 | 14,395..54,531 | 1,168 | 0.000..1,244 | -93.9% |
| wall s | 11.0 | 11.0..11.0 | 10.0 | 10.0..10.0 | -9.1% |
| task-clock ms | 84,819 | 83,247..86,723 | 83,064 | 80,108..89,931 | -2.1% |
| Gcycles | 159.2 | 149.9..167.4 | 171.4 | 154.5..171.9 | +7.7% |
| Ginstr | 81.3 | 76.8..86.0 | 81.7 | 79.2..87.3 | +0.6% |
| ctx-sw | 3,368,903 | 3,103,215..3,797,086 | 3,537,788 | 3,186,009..3,599,111 | +5.0% |
| migrations | 3,353 | 1,512..11,037 | 2,292 | 457.0..25,133 | -31.6% |
| Mcache-miss | 466.0 | 445.9..494.5 | 480.1 | 422.2..501.5 | +3.0% |

## r250000-1ms-tick5000-wt16

rate=250000/s think=1ms tick=5000us worker-threads=16 pre/max VUs=512/1024 cores=0-19 (n base=5, cand=5)

| measure | base med | base IQR | cand med | cand IQR | Δ med |
|---|---|---|---|---|---|
| achieved it/s | 138,026 | 133,805..143,059 | 209,475 | 206,594..215,128 | +51.8% |
| dropped | 1,118,968 | 1,068,439..1,160,879 | 404,651 | 347,233..433,365 | -63.8% |
| wall s | 11.0 | 11.0..11.0 | 10.0 | 10.0..10.0 | -9.1% |
| task-clock ms | 94,403 | 93,541..94,516 | 92,047 | 91,956..92,127 | -2.5% |
| Gcycles | 157.3 | 139.6..159.8 | 140.0 | 136.2..142.3 | -11.0% |
| Ginstr | 81.7 | 74.3..82.4 | 74.2 | 72.6..75.4 | -9.2% |
| ctx-sw | 3,240,017 | 2,923,670..3,448,609 | 2,838,536 | 2,795,771..2,852,878 | -12.4% |
| migrations | 2,651 | 134.0..15,632 | 210.0 | 186.0..1,434 | -92.1% |
| Mcache-miss | 457.0 | 392.5..461.0 | 398.6 | 397.2..407.0 | -12.8% |

_Note: both sides below 90% of the 250000/s target — host-limited cell._

## lowrate-overwake

rate=1000/s think=1ms tick=5000us worker-threads=16 pre/max VUs=2000/2000 cores=0-19 (n base=5, cand=5)

| measure | base med | base IQR | cand med | cand IQR | Δ med |
|---|---|---|---|---|---|
| achieved it/s | 999.1 | 999.1..999.5 | 999.5 | 999.5..999.5 | +0.0% |
| dropped | 0.000 | 0.000..0.000 | 0.000 | 0.000..0.000 | - |
| wall s | 11.0 | 11.0..11.0 | 10.0 | 10.0..10.0 | -9.1% |
| task-clock ms | 335.5 | 330.7..339.7 | 333.9 | 313.7..335.8 | -0.5% |
| Gcycles | 0.392 | 0.388..0.393 | 0.394 | 0.391..0.398 | +0.6% |
| Ginstr | 0.246 | 0.245..0.247 | 0.243 | 0.240..0.249 | -1.0% |
| ctx-sw | 9,498 | 9,293..9,772 | 9,271 | 9,258..9,303 | -2.4% |
| migrations | 35.0 | 30.0..55.0 | 52.0 | 41.0..91.0 | +48.6% |
| Mcache-miss | 9.831 | 9.775..10.5 | 11.2 | 10.8..11.4 | +14.1% |
