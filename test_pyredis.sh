#!/usr/bin/env bash
#
# Automated end-to-end test suite for pyredis.
# Spins up real server processes, drives them through the CLI, and checks
# the actual output — not a mock. Run from the outer pyredis/ folder:
#
#   chmod +x test_pyredis.sh
#   ./test_pyredis.sh
#
set -u
cd "$(dirname "$0")"

PASS=0
FAIL=0
TMPDIR=$(mktemp -d)

# Windows/Git Bash typically only has `python`, not `python3`; Linux/Mac usually
# have `python3`. Detect whichever is actually available instead of assuming.
if command -v python3 > /dev/null 2>&1; then
  PY=python3
elif command -v python > /dev/null 2>&1; then
  PY=python
else
  echo "ERROR: neither 'python3' nor 'python' found on PATH." >&2
  echo "Install Python 3.10+ and make sure it's on your PATH, then re-run this script." >&2
  exit 1
fi
echo "Using interpreter: $($PY --version)"

# Force unbuffered stdout. When Python's stdout is redirected to a file
# (not a terminal) it switches from line-buffering to block-buffering, so
# print() output can sit in memory instead of hitting disk immediately.
# That's harmless for a real user watching a terminal, but it breaks a test
# harness that greps a log file moments after the process printed to it —
# which is exactly what the pub/sub test below does. -u disables that.
export PYTHONUNBUFFERED=1
CLI="$PY -u -m pyredis.cli"
SERVER="$PY -u -m pyredis.server"

# setsid is a Linux-only utility (part of util-linux); Git Bash on Windows
# doesn't have it. Fall back to running the command directly if missing.
if command -v setsid > /dev/null 2>&1; then
  RUN_DETACHED="setsid"
else
  RUN_DETACHED=""
fi

cleanup() {
  jobs -p | xargs -r kill -9 2>/dev/null
  wait 2>/dev/null
  rm -rf "$TMPDIR"
}
trap cleanup EXIT

check() {
  # check "description" "actual" "expected"
  local desc="$1" actual="$2" expected="$3"
  if [ "$actual" == "$expected" ]; then
    echo "  PASS: $desc"
    PASS=$((PASS + 1))
  else
    echo "  FAIL: $desc"
    echo "        expected: $expected"
    echo "        actual:   $actual"
    FAIL=$((FAIL + 1))
  fi
}

start_server() {
  # start_server port maxkeys aofpath -> echoes PID
  local port=$1 maxkeys=$2 aof=$3
  $RUN_DETACHED $SERVER --port "$port" --maxkeys "$maxkeys" --aof "$aof" \
      > "$TMPDIR/server_$port.log" 2>&1 < /dev/null &
  echo $!
  sleep 1
}

echo "=============================================="
echo "1. Basic SET / GET / DEL / EXISTS"
echo "=============================================="
PID=$(start_server 7001 1000 "$TMPDIR/t1.aof")
check "SET returns OK"        "$($CLI --port 7001 SET foo bar)"      '"OK"'
check "GET returns value"     "$($CLI --port 7001 GET foo)"          '"bar"'
check "EXISTS finds key"      "$($CLI --port 7001 EXISTS foo)"       '(integer) 1'
check "DEL removes key"       "$($CLI --port 7001 DEL foo)"          '(integer) 1'
check "GET after DEL is nil"  "$($CLI --port 7001 GET foo)"          '(nil)'
check "GET missing key is nil" "$($CLI --port 7001 GET nope)"        '(nil)'
kill -9 "$PID" 2>/dev/null

echo "=============================================="
echo "2. TTL / expiry"
echo "=============================================="
PID=$(start_server 7002 1000 "$TMPDIR/t2.aof")
$CLI --port 7002 SET session abc123 EX 2 > /dev/null
TTL_VAL=$($CLI --port 7002 TTL session)
if [ "$TTL_VAL" == "(integer) 2" ] || [ "$TTL_VAL" == "(integer) 1" ]; then
  echo "  PASS: TTL reports ~2s ($TTL_VAL)"
  PASS=$((PASS + 1))
else
  echo "  FAIL: TTL reports ~2s -- got $TTL_VAL"
  FAIL=$((FAIL + 1))
fi
check "Key readable before expiry" "$($CLI --port 7002 GET session)" '"abc123"'
sleep 2.5
check "Key gone after expiry" "$($CLI --port 7002 GET session)"      '(nil)'
check "TTL on missing key is -2" "$($CLI --port 7002 TTL session)"   '(integer) -2'
$CLI --port 7002 SET forever val > /dev/null
check "TTL with no expiry is -1" "$($CLI --port 7002 TTL forever)"   '(integer) -1'
kill -9 "$PID" 2>/dev/null

echo "=============================================="
echo "3. LRU eviction (maxkeys=3)"
echo "=============================================="
PID=$(start_server 7003 3 "$TMPDIR/t3.aof")
$CLI --port 7003 SET a 1 > /dev/null
$CLI --port 7003 SET b 2 > /dev/null
$CLI --port 7003 SET c 3 > /dev/null
$CLI --port 7003 SET d 4 > /dev/null   # should evict "a" (least recently used)
DBSIZE=$($CLI --port 7003 DBSIZE)
check "Store stays at capacity" "$DBSIZE" '(integer) 3'
check "Oldest key evicted"    "$($CLI --port 7003 GET a)"            '(nil)'
check "Newest key present"    "$($CLI --port 7003 GET d)"            '"4"'
# touch "b" to make it recently-used, then add another -> "c" should be evicted, not "b"
$CLI --port 7003 GET b > /dev/null
$CLI --port 7003 SET e 5 > /dev/null
check "Touched key survives"  "$($CLI --port 7003 GET b)"            '"2"'
check "Untouched key evicted" "$($CLI --port 7003 GET c)"            '(nil)'
kill -9 "$PID" 2>/dev/null

echo "=============================================="
echo "4. AOF crash recovery (hard kill -9, then restart)"
echo "=============================================="
AOF4="$TMPDIR/t4.aof"
PID=$(start_server 7004 1000 "$AOF4")
$CLI --port 7004 SET username alice > /dev/null
$CLI --port 7004 SET counter 42 > /dev/null
kill -9 "$PID" 2>/dev/null   # simulate a real crash, no graceful shutdown
sleep 0.5
PID=$(start_server 7004 1000 "$AOF4")
check "String survives crash+restart" "$($CLI --port 7004 GET username)" '"alice"'
check "Second key survives too"       "$($CLI --port 7004 GET counter)"  '"42"'
kill -9 "$PID" 2>/dev/null

echo "=============================================="
echo "5. BGREWRITEAOF compaction"
echo "=============================================="
AOF5="$TMPDIR/t5.aof"
PID=$(start_server 7005 1000 "$AOF5")
for i in $(seq 1 10); do $CLI --port 7005 SET "k$i" "v$i" > /dev/null; done
$CLI --port 7005 DEL k1 k2 k3 > /dev/null
SIZE_BEFORE=$(wc -c < "$AOF5")
$CLI --port 7005 BGREWRITEAOF > /dev/null
SIZE_AFTER=$(wc -c < "$AOF5")
if [ "$SIZE_AFTER" -lt "$SIZE_BEFORE" ]; then
  echo "  PASS: AOF shrank after rewrite ($SIZE_BEFORE -> $SIZE_AFTER bytes)"
  PASS=$((PASS + 1))
else
  echo "  FAIL: AOF did not shrink ($SIZE_BEFORE -> $SIZE_AFTER bytes)"
  FAIL=$((FAIL + 1))
fi
check "Data intact after rewrite" "$($CLI --port 7005 DBSIZE)" '(integer) 7'
kill -9 "$PID" 2>/dev/null

echo "=============================================="
echo "6. Pub/Sub"
echo "=============================================="
PID=$(start_server 7006 1000 "$TMPDIR/t6.aof")
$RUN_DETACHED $CLI --port 7006 SUBSCRIBE news > "$TMPDIR/sub.log" 2>&1 < /dev/null &
SUBPID=$!
sleep 0.5
RECEIVERS=$($CLI --port 7006 PUBLISH news "hello world")
check "PUBLISH reports 1 receiver" "$RECEIVERS" '(integer) 1'
sleep 1
if grep -q "hello world" "$TMPDIR/sub.log"; then
  echo "  PASS: subscriber actually received the message"
  PASS=$((PASS + 1))
else
  echo "  FAIL: subscriber did not receive the message"
  FAIL=$((FAIL + 1))
fi
kill -9 "$SUBPID" "$PID" 2>/dev/null

echo "=============================================="
echo "7. Replication (full sync + live streaming)"
echo "=============================================="
MPID=$(start_server 7007 1000 "$TMPDIR/master.aof")
$CLI --port 7007 SET existing_key existing_value > /dev/null
RPID=$(start_server 7008 1000 "$TMPDIR/replica.aof")
$CLI --port 7008 REPLICAOF 127.0.0.1 7007 > /dev/null
sleep 1
check "Full sync copies pre-existing data" "$($CLI --port 7008 GET existing_key)" '"existing_value"'
$CLI --port 7007 SET live_key streamed > /dev/null
sleep 0.5
check "Live write streams to replica" "$($CLI --port 7008 GET live_key)" '"streamed"'
$CLI --port 7007 DEL existing_key > /dev/null
sleep 0.5
check "DEL propagates to replica" "$($CLI --port 7008 GET existing_key)" '(nil)'
kill -9 "$MPID" "$RPID" 2>/dev/null

echo "=============================================="
echo "RESULTS: $PASS passed, $FAIL failed"
echo "=============================================="
[ "$FAIL" -eq 0 ]