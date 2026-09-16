#!/usr/bin/env bash
# Paired local A/B for gRPC encode-once on rendered (templated) messages
# (goals/perf-round-three/grpc-encode-once.md, LOCAL PERFORMANCE VALIDATION).
#
# Usage: run.sh BASE_BIN CAND_BIN OUTDIR [SERVER_BIN]
#
# Cells: templated unary echo at ~200 B / ~2 KB / ~64 KB encoded message
# size (a random bytes payload behind a `${vu}` substitution, so every call
# takes the rendered path) plus one client-streaming cell (5 medium messages
# per call), against the in-repo loadr-testserver gRPC echo (loopback, TLS
# off; build once with
# `cargo build --release -p loadr-testserver --example grpc_echo` and pass
# the same server binary for every side). One discarded warm-up per binary
# per cell, then PAIRS measured pairs in alternating order (A,B B,A ...).
# loadr and the echo server are pinned to disjoint cores. Every run is
# wrapped in `perf stat`; one row per measured run in OUTDIR/runs.csv, no
# selection. Reduce with reduce.py.
set -euo pipefail

BASE_BIN=${1:?usage: run.sh BASE_BIN CAND_BIN OUTDIR [SERVER_BIN]}
CAND_BIN=${2:?usage: run.sh BASE_BIN CAND_BIN OUTDIR [SERVER_BIN]}
OUTDIR=${3:?usage: run.sh BASE_BIN CAND_BIN OUTDIR [SERVER_BIN]}
SERVER_BIN=${4:-target/release/examples/grpc_echo}

DURATION=10s
DURATION_S=10
PAIRS=${PAIRS:-5}
VUS=${VUS:-16}
LOADR_CORES=${LOADR_CORES:-0-7}
SERVER_CORES=${SERVER_CORES:-10-13}
PERF_EVENTS=task-clock,cycles,instructions,context-switches,cpu-migrations,cache-misses
PROTO="$(cd "$(dirname "$0")" && pwd)/echo.proto"

mkdir -p "$OUTDIR"
CSV="$OUTDIR/runs.csv"
echo "cell,binary,pair,pos,method,payload_bytes,messages,vus,cores,exit,iterations,achieved_per_s,wall_s,task_clock_ms,cycles,instructions,ctx_switches,cpu_migrations,cache_misses" > "$CSV"

{
  echo "date: $(date -u +%FT%TZ)"
  echo "host: $(hostname)"
  echo "cpu: $(lscpu | awk -F: '/Model name/ {gsub(/^ +/,"",$2); print $2}')"
  echo "governor: $(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || echo n/a)"
  echo "base: $BASE_BIN ($(sha256sum "$BASE_BIN" | cut -d' ' -f1))"
  echo "cand: $CAND_BIN ($(sha256sum "$CAND_BIN" | cut -d' ' -f1))"
  echo "server: $SERVER_BIN ($(sha256sum "$SERVER_BIN" | cut -d' ' -f1))"
  echo "kernel: $(uname -r)"
  echo "perf: $(perf --version)"
} > "$OUTDIR/meta.txt"

# Shared echo server, identical binary for both sides.
taskset -c "$SERVER_CORES" "$SERVER_BIN" > "$OUTDIR/server.log" 2>&1 &
SERVER_PID=$!
trap 'kill $SERVER_PID 2>/dev/null || true' EXIT
ADDR=""
for _ in $(seq 1 50); do
  ADDR=$(awk '/^LISTENING/ {print $2}' "$OUTDIR/server.log" 2>/dev/null || true)
  [[ -n $ADDR ]] && break
  sleep 0.1
done
[[ -n $ADDR ]] || { echo "echo server did not start (see $OUTDIR/server.log)" >&2; exit 1; }
echo "echo server at $ADDR (pid $SERVER_PID, cores $SERVER_CORES)"

# Fixed random payloads, constant for the whole invocation so every pair
# renders and encodes identical messages.
PAYLOAD_SMALL=$(head -c 192 /dev/urandom | base64 -w0)
PAYLOAD_MEDIUM=$(head -c 2000 /dev/urandom | base64 -w0)
PAYLOAD_LARGE=$(head -c 64000 /dev/urandom | base64 -w0)

payload_for() { # payload_for PAYLOAD_BYTES
  case $1 in
    192) echo "$PAYLOAD_SMALL" ;;
    2000) echo "$PAYLOAD_MEDIUM" ;;
    64000) echo "$PAYLOAD_LARGE" ;;
    *) echo "unknown payload size $1" >&2; exit 1 ;;
  esac
}

plan_unary() { # plan_unary FILE PAYLOAD_B64
  cat > "$1" <<EOF
name: grpc-encode-ab
scenarios:
  s:
    executor: constant-vus
    vus: $VUS
    duration: $DURATION
    graceful_stop: 1s
    flow:
      - request:
          name: echo
          url: grpc://$ADDR
          grpc:
            proto_files: [ "$PROTO" ]
            service: loadr.test.Echo
            method: UnaryEcho
            message: { message: "vu-\${vu}", payload: "$2" }
EOF
}

plan_stream() { # plan_stream FILE PAYLOAD_B64
  cat > "$1" <<EOF
name: grpc-encode-ab
scenarios:
  s:
    executor: constant-vus
    vus: $VUS
    duration: $DURATION
    graceful_stop: 1s
    flow:
      - request:
          name: echo
          url: grpc://$ADDR
          grpc:
            proto_files: [ "$PROTO" ]
            service: loadr.test.Echo
            method: ClientStreamEcho
            messages:
              - { message: "vu-\${vu}-1", payload: "$2" }
              - { message: "vu-\${vu}-2", payload: "$2" }
              - { message: "vu-\${vu}-3", payload: "$2" }
              - { message: "vu-\${vu}-4", payload: "$2" }
              - { message: "vu-\${vu}-5", payload: "$2" }
EOF
}

# one_run CELL LABEL BIN PAIR POS METHOD PAYLOAD_BYTES NMSGS
one_run() {
  local cell=$1 label=$2 bin=$3 pair=$4 pos=$5 method=$6 payload_bytes=$7 nmsgs=$8
  local rundir="$OUTDIR/raw/$cell/${label}-p${pair}-${pos}"
  mkdir -p "$rundir"
  if [[ $method == ClientStreamEcho ]]; then
    plan_stream "$rundir/plan.yaml" "$(payload_for "$payload_bytes")"
  else
    plan_unary "$rundir/plan.yaml" "$(payload_for "$payload_bytes")"
  fi
  local t0 t1 status=0
  t0=$(date +%s.%N)
  taskset -c "$LOADR_CORES" \
    perf stat -x, -e "$PERF_EVENTS" -o "$rundir/perf.csv" -- \
    "$bin" run --quiet --summary-export "$rundir/summary.json" "$rundir/plan.yaml" \
    > "$rundir/stdout.log" 2> "$rundir/stderr.log" || status=$?
  t1=$(date +%s.%N)
  local wall iterations achieved
  wall=$(echo "$t1 $t0" | awk '{printf "%.3f", $1-$2}')
  if [[ $status -eq 0 && -f "$rundir/summary.json" ]]; then
    iterations=$(jq '[.metrics[] | select(.metric=="iterations") | .agg.sum] | add // 0' "$rundir/summary.json")
  else
    iterations=0
  fi
  achieved=$(echo "$iterations $DURATION_S" | awk '{printf "%.1f", $1/$2}')
  # perf -x, rows: value,unit,event,... — pull each counter by event name
  # (paranoid>0 appends a :u modifier).
  pc() { awk -F, -v ev="$1" '$3==ev || $3==ev":u" {gsub(/ /,"",$1); print $1; found=1} END {if (!found) print ""}' "$rundir/perf.csv"; }
  echo "$cell,$label,$pair,$pos,$method,$payload_bytes,$nmsgs,$VUS,$LOADR_CORES,$status,$iterations,$achieved,$wall,$(pc task-clock),$(pc cycles),$(pc instructions),$(pc context-switches),$(pc cpu-migrations),$(pc cache-misses)" >> "$CSV"
  if [[ $status -ne 0 ]]; then
    echo "WARN: $cell $label pair=$pair pos=$pos exited $status (see $rundir/stderr.log)" >&2
  fi
}

# cell CELL METHOD PAYLOAD_BYTES NMSGS
cell() {
  local cell=$1 method=$2 payload_bytes=$3 nmsgs=$4
  echo "=== cell $cell (method=$method payload=${payload_bytes}B messages=$nmsgs vus=$VUS cores=$LOADR_CORES)"
  one_run "$cell" base "$BASE_BIN" 0 warmup "$method" "$payload_bytes" "$nmsgs"
  one_run "$cell" cand "$CAND_BIN" 0 warmup "$method" "$payload_bytes" "$nmsgs"
  for pair in $(seq 1 "$PAIRS"); do
    local first=base second=cand fb="$BASE_BIN" sb="$CAND_BIN"
    if (( pair % 2 == 0 )); then
      first=cand second=base fb="$CAND_BIN" sb="$BASE_BIN"
    fi
    one_run "$cell" "$first" "$fb" "$pair" 1 "$method" "$payload_bytes" "$nmsgs"
    one_run "$cell" "$second" "$sb" "$pair" 2 "$method" "$payload_bytes" "$nmsgs"
  done
}

# Warm-up rows carry pair=0/pos=warmup and are excluded by the reducer.
cell unary-small UnaryEcho 192 1
cell unary-medium UnaryEcho 2000 1
cell unary-large UnaryEcho 64000 1
cell stream-medium ClientStreamEcho 2000 5

echo "done: $CSV"
