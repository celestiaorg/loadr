#!/usr/bin/env bash
# Paired local A/B for the claim-budget arrival dispatcher
# (goals/perf-round-two/dispatcher-idle-ring.md, LOCAL PERFORMANCE VALIDATION).
#
# Usage: run.sh BASE_BIN CAND_BIN OUTDIR
#
# Runs the full 25-cell matrix — tick {1000,5000}us x worker-threads {2,16}
# x think {0s,1ms} x rate {50k,150k,250k}, plus the low-rate/high-preallocated
# broadcast over-waking case — with one discarded warm-up per binary per cell
# and 5 measured pairs in alternating order (A,B B,A A,B B,A A,B). Both sides
# of a pair are pinned to the same physical cores. Every run is wrapped in
# `perf stat`; results land in OUTDIR/runs.csv (one row per measured run, no
# selection). Reduce with reduce.py.
set -euo pipefail

BASE_BIN=${1:?usage: run.sh BASE_BIN CAND_BIN OUTDIR}
CAND_BIN=${2:?usage: run.sh BASE_BIN CAND_BIN OUTDIR}
OUTDIR=${3:?usage: run.sh BASE_BIN CAND_BIN OUTDIR}

DURATION=10s
DURATION_S=10
PAIRS=5
PERF_EVENTS=task-clock,cycles,instructions,context-switches,cpu-migrations,cache-misses

mkdir -p "$OUTDIR"
CSV="$OUTDIR/runs.csv"
echo "cell,binary,pair,pos,rate,think,tick_us,worker_threads,pre_vus,max_vus,cores,exit,iterations,dropped,achieved_per_s,wall_s,task_clock_ms,cycles,instructions,ctx_switches,cpu_migrations,cache_misses" > "$CSV"

{
  echo "date: $(date -u +%FT%TZ)"
  echo "host: $(hostname)"
  echo "cpu: $(lscpu | awk -F: '/Model name/ {gsub(/^ +/,"",$2); print $2}')"
  echo "governor: $(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || echo n/a)"
  echo "base: $BASE_BIN ($(sha256sum "$BASE_BIN" | cut -d' ' -f1))"
  echo "cand: $CAND_BIN ($(sha256sum "$CAND_BIN" | cut -d' ' -f1))"
  echo "kernel: $(uname -r)"
  echo "perf: $(perf --version)"
} > "$OUTDIR/meta.txt"

plan() { # plan FILE RATE THINK PRE MAX
  cat > "$1" <<EOF
name: dispatcher-ab
scenarios:
  s:
    executor: constant-arrival-rate
    rate: $2
    duration: $DURATION
    pre_allocated_vus: $4
    max_vus: $5
    graceful_stop: 1s
    flow:
      - think_time: { type: constant, duration: $3 }
EOF
}

# one_run CELL BINARY_LABEL BIN PAIR POS RATE THINK TICK WT PRE MAX CORES
one_run() {
  local cell=$1 label=$2 bin=$3 pair=$4 pos=$5 rate=$6 think=$7 tick=$8 wt=$9 pre=${10} max=${11} cores=${12}
  local rundir="$OUTDIR/raw/$cell/${label}-p${pair}-${pos}"
  mkdir -p "$rundir"
  plan "$rundir/plan.yaml" "$rate" "$think" "$pre" "$max"
  local t0 t1 status=0
  t0=$(date +%s.%N)
  LOADR_DISPATCH_TICK_US=$tick taskset -c "$cores" \
    perf stat -x, -e "$PERF_EVENTS" -o "$rundir/perf.csv" -- \
    "$bin" run --quiet --worker-threads "$wt" \
    --summary-export "$rundir/summary.json" "$rundir/plan.yaml" \
    > "$rundir/stdout.log" 2> "$rundir/stderr.log" || status=$?
  t1=$(date +%s.%N)
  local wall iterations dropped achieved
  wall=$(echo "$t1 $t0" | awk '{printf "%.3f", $1-$2}')
  if [[ $status -eq 0 && -f "$rundir/summary.json" ]]; then
    iterations=$(jq '[.metrics[] | select(.metric=="iterations") | .agg.sum] | add // 0' "$rundir/summary.json")
    dropped=$(jq '[.metrics[] | select(.metric=="dropped_iterations") | .agg.sum] | add // 0' "$rundir/summary.json")
  else
    iterations=0; dropped=0
  fi
  achieved=$(echo "$iterations $DURATION_S" | awk '{printf "%.1f", $1/$2}')
  # perf -x, rows: value,unit,event,... — pull each counter by event name.
  pc() { awk -F, -v ev="$1" '$3==ev {gsub(/ /,"",$1); print $1; found=1} END {if (!found) print ""}' "$rundir/perf.csv"; }
  echo "$cell,$label,$pair,$pos,$rate,$think,$tick,$wt,$pre,$max,$cores,$status,$iterations,$dropped,$achieved,$wall,$(pc task-clock),$(pc cycles),$(pc instructions),$(pc context-switches),$(pc cpu-migrations),$(pc cache-misses)" >> "$CSV"
  if [[ $status -ne 0 ]]; then
    echo "WARN: $cell $label pair=$pair pos=$pos exited $status (see $rundir/stderr.log)" >&2
  fi
}

# cell CELL RATE THINK TICK WT PRE MAX
cell() {
  local cell=$1 rate=$2 think=$3 tick=$4 wt=$5 pre=$6 max=$7 cores
  case $wt in
    2) cores=0-3 ;;
    16) cores=0-19 ;;
    *) cores=0-$((wt + 3)) ;;
  esac
  echo "=== cell $cell (rate=$rate think=$think tick=${tick}us wt=$wt pre=$pre max=$max cores=$cores)"
  one_run "$cell" base "$BASE_BIN" 0 warmup "$rate" "$think" "$tick" "$wt" "$pre" "$max" "$cores"
  one_run "$cell" cand "$CAND_BIN" 0 warmup "$rate" "$think" "$tick" "$wt" "$pre" "$max" "$cores"
  for pair in $(seq 1 $PAIRS); do
    local first=base second=cand fb="$BASE_BIN" sb="$CAND_BIN"
    if (( pair % 2 == 0 )); then
      first=cand second=base fb="$CAND_BIN" sb="$BASE_BIN"
    fi
    one_run "$cell" "$first" "$fb" "$pair" 1 "$rate" "$think" "$tick" "$wt" "$pre" "$max" "$cores"
    one_run "$cell" "$second" "$sb" "$pair" 2 "$rate" "$think" "$tick" "$wt" "$pre" "$max" "$cores"
  done
}

# Warm-up rows carry pair=0/pos=warmup and are excluded by the reducer.
for tick in 1000 5000; do
  for wt in 2 16; do
    for think in 0s 1ms; do
      # VU sizing identical within every pair: zero-think iterations barely
      # overlap ticks; 1ms think keeps rate/1000 VUs in flight.
      local_pre=64; local_max=128
      if [[ $think == 1ms ]]; then local_pre=512; local_max=1024; fi
      for rate in 50000 150000 250000; do
        cell "r${rate}-${think}-tick${tick}-wt${wt}" "$rate" "$think" "$tick" "$wt" "$local_pre" "$local_max"
      done
    done
  done
done
# Broadcast over-waking exposure: tiny budgets against a huge parked pool.
cell "lowrate-overwake" 1000 1ms 5000 16 2000 2000

echo "done: $CSV"
