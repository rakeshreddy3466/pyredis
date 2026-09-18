"""
pyredis benchmark tool -- measures throughput and latency, the same idea
as the real `redis-benchmark` tool.

Why not just time a loop of CLI calls? Because each `pyredis.cli` invocation
pays Python interpreter startup + a fresh TCP handshake every single time --
that overhead would dominate the measurement and tell you nothing about the
server itself. A real benchmark opens N persistent connections up front and
fires many commands down each one, so what you're measuring is actually the
server's per-command work, not process startup cost.

Usage:
    python3 -m pyredis.benchmark --port 6380 -c 50 -n 20000 -t set,get,mixed
"""

import argparse
import asyncio
import random
import statistics
import time

from .protocol import enc_command
from .cli import read_reply


async def worker(host, port, n_requests, keyspace, command, value, latencies):
    """One simulated client: opens a single persistent connection and fires
    n_requests commands down it sequentially, recording per-command latency."""
    reader, writer = await asyncio.open_connection(host, port)
    try:
        for _ in range(n_requests):
            key = f"key:{random.randint(0, keyspace - 1)}"
            if command == "set":
                parts = ["SET", key, value]
            elif command == "get":
                parts = ["GET", key]
            else:  # mixed: 50/50 read/write, the realistic case for most apps
                parts = ["SET", key, value] if random.random() < 0.5 else ["GET", key]

            start = time.perf_counter()
            writer.write(enc_command(parts))
            await writer.drain()
            await read_reply(reader)
            latencies.append((time.perf_counter() - start) * 1000)  # ms
    finally:
        writer.close()


async def run_benchmark(host, port, clients, total_requests, keyspace, command, value_size):
    per_client = max(1, total_requests // clients)
    value = "x" * value_size
    latency_buckets = [[] for _ in range(clients)]

    start = time.perf_counter()
    await asyncio.gather(*[
        worker(host, port, per_client, keyspace, command, value, latency_buckets[i])
        for i in range(clients)
    ])
    elapsed = time.perf_counter() - start

    all_latencies = [lat for bucket in latency_buckets for lat in bucket]
    return elapsed, all_latencies


def percentile(sorted_values, pct):
    if not sorted_values:
        return 0.0
    k = (len(sorted_values) - 1) * pct
    lo, hi = int(k), min(int(k) + 1, len(sorted_values) - 1)
    if lo == hi:
        return sorted_values[lo]
    return sorted_values[lo] + (sorted_values[hi] - sorted_values[lo]) * (k - lo)


def print_report(command, clients, elapsed, latencies):
    n = len(latencies)
    if n == 0:
        print(f"  no completed requests for '{command}' -- is the server reachable?")
        return
    sorted_lat = sorted(latencies)
    ops_per_sec = n / elapsed if elapsed > 0 else 0.0

    print(f"\n=== {command.upper()} ===")
    print(f"  concurrency:   {clients} clients")
    print(f"  requests:      {n}")
    print(f"  total time:    {elapsed:.3f}s")
    print(f"  throughput:    {ops_per_sec:,.0f} requests/sec")
    print(f"  latency (ms):  avg={statistics.mean(latencies):.3f}  "
          f"p50={percentile(sorted_lat, 0.50):.3f}  "
          f"p95={percentile(sorted_lat, 0.95):.3f}  "
          f"p99={percentile(sorted_lat, 0.99):.3f}  "
          f"max={sorted_lat[-1]:.3f}")


async def main_async(args):
    print(f"Benchmarking {args.host}:{args.port}  "
          f"({args.clients} concurrent clients, {args.requests} total requests per test)")
    for command in args.tests:
        elapsed, latencies = await run_benchmark(
            args.host, args.port, args.clients, args.requests,
            args.keyspace, command, args.datasize,
        )
        print_report(command, args.clients, elapsed, latencies)


def main():
    parser = argparse.ArgumentParser(description="Benchmark a running pyredis server")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6380)
    parser.add_argument("-c", "--clients", type=int, default=50,
                         help="number of concurrent connections (default: 50)")
    parser.add_argument("-n", "--requests", type=int, default=20000,
                         help="total requests per test, split across clients (default: 20000)")
    parser.add_argument("-d", "--datasize", type=int, default=3,
                         help="value size in bytes for SET commands (default: 3)")
    parser.add_argument("--keyspace", type=int, default=10000,
                         help="number of distinct keys to randomize over (default: 10000)")
    parser.add_argument("-t", "--tests", default="set,get,mixed",
                         help="comma-separated: set,get,mixed (default: set,get,mixed)")
    args = parser.parse_args()
    args.tests = [t.strip() for t in args.tests.split(",") if t.strip()]

    try:
        asyncio.run(main_async(args))
    except (ConnectionRefusedError, OSError) as e:
        print(f"Could not connect to {args.host}:{args.port}: {e}")


if __name__ == "__main__":
    main()