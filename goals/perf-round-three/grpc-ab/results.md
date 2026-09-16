# gRPC encode-once: local A/B evidence

Candidate: `nikolai/perf-grpc-encode-once` (de-clone deserialize, encode each
rendered message once at execute time, `bytes_sent` from buffer length,
`Outbound` retired to plain `Bytes`). Base: `1c3d683` (tip of
`nikolai/perf-grpc-call-cache-memoization`, the branch this one extends).
The sibling `grpc-template-precompile` was not in flight anywhere at
measurement time, so the base contains no part of it.

**Verdict.** Per-iteration CPU work drops a consistent −0.8..−1.4%
(instructions/iteration, tight IQRs) on the small/medium/stream cells and is
exactly flat (−0.1%) at 64 KB, where the removed metric-only `encoded_len`
walk (cheap for one big bytes field) is offset by the added exact-size
buffer + memcpy. Throughput: **small +6.9%, medium +1.2%, stream +4.6%,
large flat within (large) host noise**. The expected "win grows with message
size" did *not* materialize: the win concentrates where per-field walk
overhead dominates (many small fields), not where one bytes field dominates.
No cell regressed on per-iteration CPU.

## Setup

- host `tower`, AMD Ryzen 9 3950X (16C), kernel 7.0.14-arch1-1, perf 7.1.1-1,
  governor `performance` (amd-pstate-epp). Boost clocks under sustained
  all-core load visibly wander 1.75–3.4 GHz, which drives the ±10..15%
  run-to-run throughput dispersion in the CPU-saturated 64 KB cell (see
  below); instruction counts are unaffected.
- loadr pinned `taskset -c 0-7`; echo server pinned `10-13` (disjoint).
- server: `loadr-testserver` gRPC echo via `examples/grpc_echo.rs` from this
  branch, loopback, TLS off, one instance shared by both sides of every pair.
  Requires the branch's TCP_NODELAY fix — without it any message larger than
  one coalesced write is delayed-ACK-bound at ~100 it/s and measures nothing.
- binaries (`cargo +1.93.0 build --release --locked -p loadr-cli`):
  - base `1c3d683` sha256 `149913d4…f11718`
  - candidate (encode_to_vec) sha256 `fd6f3bfa…100ef0`
  - candidate-B scratch (BytesMut, strategy comparison only, never pushed)
    sha256 `48d5835c…55e466`
  - server example sha256 `bc1cf9a0…70a62d`
- `perf stat` events counted user-space only (`perf_event_paranoid=2`), so
  ctx-switch/migration rows read 0 — ignore them; task-clock/cycles/
  instructions/cache-misses are the meaningful counters.

## Method

Closed model (`constant-vus`, 16 VUs, 10 s, `--quiet --summary-export`, no
sample-consuming output). Messages are templated (`"vu-${vu}"`) so every call
takes the rendered path; payload is a fixed random base64 blob per session.
Cells: unary ~200 B / ~2 KB / ~64 KB encoded, plus client-streaming with
5×2 KB messages per call. Per cell: one discarded warm-up per binary, then 5
measured pairs alternating A,B B,A … (10 measured runs/cell/side-pair).
Reducer (`reduce.py`) reports median + IQR over all measured runs, no
selection; `instr/iter` is derived per run. Raw rows: `runs.csv` (final),
`runs-strategy.csv`, `runs-large-rerun.csv`.

## Encoder strategy comparison (required pre-step)

`encode_to_vec` (V: `encoded_len` walk + exact alloc + `encode_raw`) vs
`encode_raw` into a growable `BytesMut` (B: one walk, doubling reallocs).
Same matrix, V labelled `base`, B labelled `cand` in `runs-strategy.csv`:

| cell | achieved Δ (B vs V) | instr/iter Δ |
|---|---|---|
| unary-small | +0.2% | −0.9% |
| unary-medium | −0.3% | −0.5% |
| unary-large | −3.9% (IQRs fully overlap) | −0.0% |
| stream-medium | −0.5% | −0.6% |

No wall-clock winner anywhere; B's sub-1% instruction edge at small/medium
never reaches throughput and vanishes at 64 KB where its doubling reallocs
copy ~2× the payload. **Selected: V (`encode_to_vec`)** — exact-size
allocation, measured equal-or-better at large sizes, and the same call the
literal cache already uses. Traversal count alone indeed does not pick the
winner.

## Final A/B: base 1c3d683 vs candidate (V)

### unary-small — payload 192 B (encoded ≈ 200 B)

| measure | base med | base IQR | cand med | cand IQR | Δ med |
|---|---|---|---|---|---|
| achieved it/s | 67,247 | 65,926..70,200 | 71,911 | 71,885..72,000 | **+6.9%** |
| task-clock ms | 49,456 | 48,215..56,259 | 57,564 | 57,209..57,659 | +16.4% |
| Ginstr | 93.9 | 92.0..98.5 | 99.0 | 99.0..99.1 | +5.5% |
| instr/iter | 139,623 | 139,618..140,212 | 137,602 | 137,578..137,724 | **−1.4%** |
| Mcache-miss | 9,469 | 9,294..10,479 | 10,466 | 10,382..10,475 | +10.5% |

The feared small-message memcpy trade did not materialize as a regression —
this is the *best* throughput cell. Neither side saturates 8 cores here
(task-clock ≤ 58 s of 80 core-seconds); the candidate's lower per-call work
and allocation churn let closed-loop VUs iterate faster, so its total CPU and
cache-miss counts rise with the extra iterations while per-iteration
instructions fall.

### unary-medium — payload 2,000 B (encoded ≈ 2 KB)

| measure | base med | base IQR | cand med | cand IQR | Δ med |
|---|---|---|---|---|---|
| achieved it/s | 54,322 | 53,350..54,349 | 54,977 | 53,755..54,998 | **+1.2%** |
| task-clock ms | 57,973 | 57,167..58,530 | 56,557 | 55,706..58,792 | −2.4% |
| Ginstr | 178.5 | 175.2..178.5 | 179.1 | 175.2..179.2 | +0.4% |
| instr/iter | 328,426 | 328,412..328,450 | 325,861 | 325,678..326,008 | **−0.8%** |
| Mcache-miss | 8,436 | 8,401..8,492 | 8,287 | 8,146..8,476 | −1.8% |

### unary-large — payload 64,000 B (encoded ≈ 64 KB)

| measure | base med | base IQR | cand med | cand IQR | Δ med |
|---|---|---|---|---|---|
| achieved it/s | 10,722 | 10,209..10,746 | 9,504 | 9,379..9,516 | −11.4% |
| task-clock ms | 72,380 | 68,820..72,728 | 61,904 | 60,812..62,288 | −14.5% |
| Ginstr | 705.6 | 671.7..707.1 | 625.1 | 616.8..625.7 | −11.4% |
| instr/iter | 6,580,746 | 6,580,012..6,581,410 | 6,576,158 | 6,575,554..6,576,435 | **−0.1%** |
| Mcache-miss | 2,733 | 2,545..2,739 | 2,287 | 2,244..2,305 | −16.3% |

**This cell's throughput median is not trustworthy at n=5.** It is the only
fully CPU-saturated cell and rides the boost-clock lottery: raw per-run it/s
spans 7.9k–11.3k across sessions on *both* sides. Three sessions with the
same binaries: −11.4% (above), **+6.6%** (10-pair rerun,
`runs-large-rerun.csv`: base med 9,395 IQR 8,836..9,983; cand med 10,017 IQR
9,389..10,827), and the strategy session where the same candidate binary
scored 10,737 med — indistinguishable from base's 10,722 here. Meanwhile
instr/iter is dead flat (−0.1%) with sub-0.01% IQRs in every session: at
64 KB the removed `encoded_len` walk (O(fields), cheap when one bytes field
dominates) is offset by the added exact-size alloc + 64 KB memcpy into the
codec buffer. Honest call: **flat at 64 KB — no measured win, no measured
regression; the throughput medians in either direction are session noise.**

### stream-medium — 5 × 2,000 B per call

| measure | base med | base IQR | cand med | cand IQR | Δ med |
|---|---|---|---|---|---|
| achieved it/s | 36,334 | 35,650..37,831 | 38,019 | 37,789..38,121 | **+4.6%** |
| task-clock ms | 66,914 | 64,193..71,309 | 70,584 | 70,153..70,911 | +5.5% |
| Ginstr | 388.3 | 380.9..404.3 | 401.2 | 398.8..402.3 | +3.3% |
| instr/iter | 1,068,603 | 1,068,443..1,068,649 | 1,055,316 | 1,055,206..1,055,343 | **−1.2%** |
| Mcache-miss | 6,457 | 6,203..6,749 | 6,644 | 6,634..6,659 | +2.9% |

## Caveats

- Both sides still re-render the template tree per call (parse-per-leaf); at
  2–64 KB that shared cost dilutes the relative encode-side delta. The
  sibling goal `grpc-template-precompile` owns it.
- The 64 KB payload arrives as an ~85 KB base64 JSON leaf; `render_json` and
  the (now removed on candidate / still present on base) value clone scale
  with it. The clone removal is part of what the small/medium/stream deltas
  measure.
- Single host, loopback. Absolute it/s are not transferable; the paired
  deltas and per-iteration instruction counts are the signal.
