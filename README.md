# pyredis

A Redis-like in-memory key-value store, built from scratch in Python to understand — not just use — how an in-memory database actually works under the hood.

It speaks real **RESP** (the actual Redis wire protocol), so it's compatible with the real `redis-cli` as an independent sanity check.

```
127.0.0.1:6380> SET session:42 alice EX 300
OK
127.0.0.1:6380> GET session:42
"alice"
127.0.0.1:6380> TTL session:42
(integer) 300
```

## Features

- **Core ops** — `SET` / `GET` / `DEL` / `EXISTS` / `KEYS` / `DBSIZE`
- **TTL / expiry** — `SET key val EX seconds`, `TTL`, `PERSIST`
- **LRU cache eviction** — bounded key capacity, O(1) evict-the-least-recently-used
- **Crash-safe persistence** — append-only log (AOF), replayed on startup, survives a hard `kill -9`
- **AOF compaction** — `BGREWRITEAOF` rewrites the log down to its minimal form
- **Non-blocking fsync** — disk sync runs on a background thread so one slow write doesn't stall every other client (see [Benchmarking](#benchmarking) below for the measured impact)
- **Pub/Sub** — `SUBSCRIBE` / `PUBLISH` / `UNSUBSCRIBE`
- **Replication** — `REPLICAOF host port`: full sync + live write streaming to a follower
- **Real RESP protocol** — same wire format as actual Redis
- **Custom CLI client** — `redis-cli`-style REPL and one-shot command mode
- **Automated test suite** — 25 assertions, including a real crash/recovery test, not just happy-path checks
- **Benchmark tool** — measures throughput and latency percentiles under concurrent load, like `redis-benchmark`

## Architecture

```
cli.py  --RESP over TCP-->  server.py (asyncio event loop)
                                 |
                        command dispatch
                            /    |    \
                     store.py  aof.py  pubsub.py
                    (LRU +     (crash-safe    (channel ->
                     TTL)       log on disk)   subscriber fan-out)
                                 |
                        replica_writers[]
                                 |
                    other server.py instances
                     running as replicas
```

Each file owns exactly one concern:

| File | Responsibility |
|---|---|
| `protocol.py` | RESP encode/decode — the wire format |
| `store.py` | The actual in-memory dict, LRU eviction, TTL |
| `aof.py` | Append-only log, replay on startup, compaction, non-blocking fsync |
| `pubsub.py` | Channel → subscriber socket fan-out |
| `server.py` | asyncio TCP server, command dispatch, replication |
| `cli.py` | Interactive client |
| `benchmark.py` | Throughput/latency load-testing tool |

## Requirements

Python 3.10+, no external dependencies (standard library only).

```bash
python3 --version   # or `python --version` on Windows
```

## Setup

```
pyredis/
├── README.md
├── test_pyredis.sh
├── .gitignore
└── pyredis/
    ├── __init__.py
    ├── protocol.py
    ├── store.py
    ├── aof.py
    ├── pubsub.py
    ├── server.py
    ├── cli.py
    └── benchmark.py
```

```bash
git clone https://github.com/narendra-sagar-velpula/pyredis.git
cd pyredis
```

## Running it

Start the server (leave this running in its own terminal):

```bash
python3 -m pyredis.server --port 6380 --maxkeys 10000 --aof data.aof
```

| Flag | Default | Meaning |
|---|---|---|
| `--host` | `0.0.0.0` | interface to bind |
| `--port` | `6380` | TCP port |
| `--maxkeys` | `10000` | keys allowed before LRU eviction kicks in |
| `--aof` | `pyredis.aof` | path to the append-only log file |

Talk to it from another terminal:

```bash
python3 -m pyredis.cli --port 6380
127.0.0.1:6380> SET name alice
OK
127.0.0.1:6380> GET name
"alice"
```

One-shot mode (no REPL):
```bash
python3 -m pyredis.cli --port 6380 GET name
```

**Windows / Git Bash:** use `python` instead of `python3` if that's what's on your `PATH`.

**Bonus:** since this speaks real RESP, the actual `redis-cli` works too:
```bash
redis-cli -p 6380 SET foo bar
```

## Command reference

```
PING [msg]              GET key                   SET key value [EX sec|PX ms]
DEL key [key...]        EXISTS key [key...]       EXPIRE key seconds
TTL key                 PERSIST key                KEYS pattern
DBSIZE                  FLUSHALL                    BGREWRITEAOF
INFO                    ECHO msg                    QUIT
SUBSCRIBE ch [ch...]    UNSUBSCRIBE [ch...]        PUBLISH ch message
REPLICAOF host port     REPLICAOF NO ONE
```

## Testing

`test_pyredis.sh` is an automated end-to-end test suite. It spins up real server processes and drives them through the actual CLI, checking real output — not mocks — then cleans up every process and temp file it created, even if a test fails partway through.

```bash
chmod +x test_pyredis.sh
./test_pyredis.sh
```

It auto-detects whether `python3` or `python` is on your `PATH` (handles the Windows/Git Bash case where only `python` exists) and skips `setsid` gracefully on platforms that don't have it.

**Covers, end to end (25 assertions):**
1. Basic `SET` / `GET` / `DEL` / `EXISTS`
2. TTL expiry — key readable before expiry, gone after, correct `TTL` values for missing/no-expiry keys
3. LRU eviction — confirms the least-recently-used key is dropped, and that touching a key protects it from being next in line
4. AOF crash recovery — a real `kill -9` (not a graceful shutdown), then restart, then verify the data survived
5. `BGREWRITEAOF` compaction — verifies the log file on disk actually shrinks
6. Pub/Sub — a real subscriber process actually receives a real published message
7. Replication — full sync of pre-existing data, live streaming of new writes, and `DEL` propagation to a replica

Exit code `0` = all passed, `1` = something broke — safe to wire into CI.

```
==============================================
RESULTS: 25 passed, 0 failed
==============================================
```

## Benchmarking

`benchmark.py` measures throughput and latency under concurrent load — the same idea as the real `redis-benchmark` tool. It opens `-c` persistent connections up front and fires commands down each one continuously, so what's measured is the server's actual per-command cost, not Python process startup or TCP handshake overhead (which is why this is a separate tool from `test_pyredis.sh`, not folded into it).

```bash
python3 -m pyredis.benchmark --port 6380 -c 50 -n 20000 -t set,get,mixed
```

| Flag | Meaning | Default |
|---|---|---|
| `-c` | concurrent connections | 50 |
| `-n` | total requests per test, split across clients | 20000 |
| `-t` | tests to run: `set`, `get`, `mixed` | all three |
| `-d` | value size in bytes for SET | 3 |
| `--keyspace` | number of distinct random keys | 10000 |

Reports throughput (requests/sec) and latency percentiles (avg, p50, p95, p99, max) per test.

### What benchmarking found, and what got fixed

The first run surfaced a real bottleneck, not a synthetic one:

| | SET throughput | avg latency | max latency |
|---|---|---|---|
| **Before fix** | 1,219 req/s | 40.6ms | 627ms |
| **After fix** | 1,600–4,100 req/s* | 12–31ms* | 53–100ms* |

*\*Range reflects that write latency is ultimately capped by the underlying disk's fsync speed — results vary by machine/storage, see note below.*

**Root cause:** every `SET` was calling `os.fsync()` synchronously on the same thread running the asyncio event loop. `fsync()` blocks until the OS confirms data is physically on disk — so while one client's `SET` was syncing, the *entire* server was frozen, including unrelated `GET`s from other clients. Concurrency didn't help because everyone was queuing behind one blocking syscall, not actually running in parallel — confirmed with Little's Law (`clients ÷ throughput ≈ avg latency`, which matched the measured numbers almost exactly).

**Fix:** `aof.py` now offloads the `fsync()` call to a background thread via `loop.run_in_executor()`, so other connections keep being served while a write is syncing to disk:

```python
async def log_async(self, parts):
    self._file.write(enc_command(parts))
    self._file.flush()
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, os.fsync, self._file.fileno())
```

This doesn't make fsync itself faster — it's still the same durability guarantee, every write is confirmed on disk before returning `OK` — it just stops one slow write from stalling every other client. That's why `GET` throughput and the worst-case (`max`) latency spikes improved the most; raw sustained `SET` throughput is still ultimately bounded by how fast the underlying disk can fsync, which is why results vary across machines (an SSD vs. a OneDrive-synced folder vs. a throttled VM disk will all fsync at very different speeds).

### Try it yourself

```bash
rm -f data.aof   # fresh AOF so results aren't skewed by leftover data
python3 -m pyredis.server --port 6380 --maxkeys 50000 --aof data.aof
python3 -m pyredis.benchmark --port 6380 -c 50 -n 20000 -t set,get,mixed
```

Push concurrency further to find where the next bottleneck shows up (the default thread pool executor caps out around `min(32, cpu_count + 4)` worker threads):

```bash
python3 -m pyredis.benchmark --port 6380 -c 100 -n 50000 -t set,get,mixed
```

## Try the core features yourself

**LRU eviction:**
```bash
python3 -m pyredis.server --port 6380 --maxkeys 3 --aof /tmp/demo.aof
python3 -m pyredis.cli --port 6380 SET a 1
python3 -m pyredis.cli --port 6380 SET b 2
python3 -m pyredis.cli --port 6380 SET c 3
python3 -m pyredis.cli --port 6380 SET d 4   # evicts "a"
python3 -m pyredis.cli --port 6380 KEYS "*"  # b, c, d
```

**Crash recovery:**
```bash
kill -9 <server_pid>                                   # simulate a real crash
python3 -m pyredis.server --port 6380 --aof data.aof    # restart, data replays back in
```

**Replication:**
```bash
python3 -m pyredis.server --port 6400 --aof master.aof     # terminal A
python3 -m pyredis.server --port 6401 --aof replica.aof    # terminal B
python3 -m pyredis.cli --port 6401 REPLICAOF 127.0.0.1 6400
python3 -m pyredis.cli --port 6400 SET k v
python3 -m pyredis.cli --port 6401 GET k   # "v" -- streamed live from the master
```

**Pub/Sub** — note this is fire-and-forget, not a queue: a subscriber only receives messages published *after* it subscribes, nothing is buffered for late joiners.
```bash
python3 -m pyredis.cli --port 6380 SUBSCRIBE news      # terminal A -- subscribe first
python3 -m pyredis.cli --port 6380 PUBLISH news hello   # terminal B -- then publish
```

## How it works (the short version)

- **LRU eviction**: `OrderedDict` — every access moves a key to the end (most recent); over capacity, pop from the front (least recent). O(1) both ways.
- **Crash safety**: every write is appended to a log file as the exact command that produced it, then `fsync`'d to force it out of OS buffers onto physical disk before continuing. On restart, the log is replayed top to bottom.
- **Non-blocking durability**: the fsync call runs on a background thread pool instead of the event loop thread, so a slow disk sync doesn't stall unrelated client connections.
- **Replication**: log shipping — a replica connects, receives a full snapshot, then the connection stays open and every future write on the master streams down it live.
- **Concurrency**: a single-threaded `asyncio` event loop handles many simultaneous client connections by switching between them during I/O waits, instead of spawning a thread per connection.

## Known limitations

This is a learning project, not a production database — worth being upfront about the gaps:

- Single-threaded Python event loop — not intended to compete with real Redis's raw throughput
- Write throughput is bounded by disk fsync speed; no `everysec`/`no` fsync policy option yet (Redis offers these as a durability/speed tradeoff)
- No AUTH / TLS
- No automatic failover if a master goes down
- LRU eviction is by key *count*, not actual byte-size of values
- Strings only — no lists, hashes, sets, or sorted sets
- Replication has no reconnect/resume logic — a dropped replica needs a fresh full sync
