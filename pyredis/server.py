"""
pyredis server: an asyncio TCP server speaking RESP, backed by an
in-memory LRU store, with AOF persistence, pub/sub, and basic
leader -> replica replication.

Run it:
    python -m pyredis.server --port 6380 --maxkeys 10000 --aof data.aof

Connect to it:
    python -m pyredis.cli --port 6380
    (or, since it's real RESP: redis-cli -p 6380)
"""

import argparse
import asyncio
import time

from .store import LRUStore
from .aof import AOF
from .pubsub import PubSub
from .protocol import read_command, enc_simple, enc_error, enc_int, enc_bulk, enc_array, enc_command

WRITE_COMMANDS = {"SET", "DEL", "EXPIRE", "PERSIST", "FLUSHALL"}


class ClientState:
    """Per-connection state."""
    def __init__(self, writer):
        self.writer = writer
        self.subscribed = False
        self.is_replica = False  # this connection is a replica streaming from us


class PyRedisServer:
    def __init__(self, max_keys=10_000, aof_path="pyredis.aof", host="0.0.0.0", port=6380):
        self.host = host
        self.port = port
        self.store = LRUStore(max_keys=max_keys)
        self.aof = AOF(aof_path)
        self.pubsub = PubSub()
        self.start_ts = time.time()

        # --- master side: connections currently streaming as our replicas ---
        self.replica_writers: list[asyncio.StreamWriter] = []

        # --- replica side: are *we* replicating from someone else? ---
        self.is_replica = False
        self.master_host = None
        self.master_port = None
        self._replica_task: asyncio.Task | None = None

        self.command_count = 0

    # ---------------------------------------------------------------- boot

    def load_aof(self):
        n = self.aof.replay(self._apply_from_log)
        if n:
            print(f"[pyredis] replayed {n} commands from AOF, {self.store.dbsize()} keys loaded")

    def _apply_from_log(self, args):
        """Apply a command coming from the AOF (startup replay) or from a
        master (replication stream) directly to the store, no response,
        no re-logging (avoid double-writing our own AOF while replaying it)."""
        if not args:
            return
        cmd = args[0].upper()
        if cmd == "SET":
            key, value = args[1], args[2]
            ttl = None
            if len(args) >= 5 and args[3].upper() == "EX":
                ttl = float(args[4])
            elif len(args) >= 5 and args[3].upper() == "PX":
                ttl = float(args[4]) / 1000.0
            self.store.set(key, value, ttl)
        elif cmd == "DEL":
            self.store.delete(*args[1:])
        elif cmd == "EXPIRE":
            self.store.expire(args[1], float(args[2]))
        elif cmd == "PERSIST":
            self.store.persist(args[1])
        elif cmd == "FLUSHALL":
            self.store.flushall()

    # ---------------------------------------------------------- propagation

    async def _log_and_propagate(self, parts):
        """Write to our own AOF (non-blocking fsync via thread pool) and
        forward to any connected replicas."""
        await self.aof.log_async(parts)
        if self.replica_writers:
            encoded = enc_command(parts)
            dead = []
            for w in self.replica_writers:
                try:
                    w.write(encoded)
                except (ConnectionError, OSError):
                    dead.append(w)
            for w in dead:
                self.replica_writers.remove(w)

    # --------------------------------------------------------- serving loop

    async def start(self):
        self.load_aof()
        server = await asyncio.start_server(self._handle_client, self.host, self.port)
        addr = server.sockets[0].getsockname()
        print(f"[pyredis] listening on {addr[0]}:{addr[1]}  (maxkeys={self.store.max_keys})")
        async with server:
            await server.serve_forever()

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        peer = writer.get_extra_info("peername")
        state = ClientState(writer)
        try:
            while True:
                try:
                    args = await read_command(reader)
                except (asyncio.IncompleteReadError, ConnectionResetError):
                    break
                if args is None:
                    break
                if not args:
                    continue
                if state.is_replica:
                    # This connection has become a replica stream; we don't
                    # expect further commands from it (real Redis would
                    # accept REPLCONF ACK here, we keep it simple).
                    continue

                response = await self._dispatch(args, state)
                if response is not None:
                    writer.write(response)
                    await writer.drain()
                if args[0].upper() == "QUIT":
                    break
        finally:
            self.pubsub.unsubscribe_all(writer)
            if writer in self.replica_writers:
                self.replica_writers.remove(writer)
                print(f"[pyredis] replica {peer} disconnected")
            writer.close()

    # -------------------------------------------------------------- dispatch

    async def _dispatch(self, args, state: ClientState):
        cmd = args[0].upper()
        self.command_count += 1

        try:
            if cmd == "PING":
                return enc_simple("PONG") if len(args) == 1 else enc_bulk(args[1])

            if cmd == "ECHO":
                return enc_bulk(args[1]) if len(args) > 1 else enc_error("ERR wrong number of arguments")

            if cmd == "QUIT":
                return enc_simple("OK")

            if cmd == "SET":
                if len(args) < 3:
                    return enc_error("ERR wrong number of arguments for 'set'")
                key, value = args[1], args[2]
                ttl = None
                i = 3
                while i < len(args):
                    opt = args[i].upper()
                    if opt == "EX" and i + 1 < len(args):
                        ttl = float(args[i + 1]); i += 2
                    elif opt == "PX" and i + 1 < len(args):
                        ttl = float(args[i + 1]) / 1000.0; i += 2
                    else:
                        return enc_error("ERR syntax error")
                self.store.set(key, value, ttl)
                await self._log_and_propagate(["SET", key, value] + (["EX", str(ttl)] if ttl else []))
                return enc_simple("OK")

            if cmd == "GET":
                if len(args) != 2:
                    return enc_error("ERR wrong number of arguments for 'get'")
                return enc_bulk(self.store.get(args[1]))

            if cmd == "DEL":
                if len(args) < 2:
                    return enc_error("ERR wrong number of arguments for 'del'")
                n = self.store.delete(*args[1:])
                if n:
                    await self._log_and_propagate(["DEL"] + args[1:])
                return enc_int(n)

            if cmd == "EXISTS":
                if len(args) < 2:
                    return enc_error("ERR wrong number of arguments for 'exists'")
                return enc_int(sum(1 for k in args[1:] if self.store.exists(k)))

            if cmd == "EXPIRE":
                if len(args) != 3:
                    return enc_error("ERR wrong number of arguments for 'expire'")
                n = self.store.expire(args[1], float(args[2]))
                if n:
                    await self._log_and_propagate(["EXPIRE", args[1], args[2]])
                return enc_int(n)

            if cmd == "TTL":
                if len(args) != 2:
                    return enc_error("ERR wrong number of arguments for 'ttl'")
                return enc_int(self.store.ttl(args[1]))

            if cmd == "PERSIST":
                if len(args) != 2:
                    return enc_error("ERR wrong number of arguments for 'persist'")
                n = self.store.persist(args[1])
                if n:
                    await self._log_and_propagate(["PERSIST", args[1]])
                return enc_int(n)

            if cmd == "KEYS":
                pattern = args[1] if len(args) > 1 else "*"
                return enc_array([enc_bulk(k) for k in self.store.keys(pattern)])

            if cmd == "DBSIZE":
                return enc_int(self.store.dbsize())

            if cmd == "FLUSHALL":
                self.store.flushall()
                await self._log_and_propagate(["FLUSHALL"])
                return enc_simple("OK")

            if cmd == "BGREWRITEAOF":
                self.aof.rewrite(self.store.dump())
                return enc_simple("Background AOF rewrite finished")

            if cmd == "INFO":
                return enc_bulk(self._info_text())

            # ---- pub/sub ----
            if cmd == "SUBSCRIBE":
                if len(args) < 2:
                    return enc_error("ERR wrong number of arguments for 'subscribe'")
                state.subscribed = True
                out = b""
                for ch in args[1:]:
                    self.pubsub.subscribe(ch, state.writer)
                    n_ch = len(self.pubsub.channels_for(state.writer))
                    out += enc_array([enc_bulk("subscribe"), enc_bulk(ch), enc_int(n_ch)])
                return out

            if cmd == "UNSUBSCRIBE":
                channels = args[1:] if len(args) > 1 else self.pubsub.channels_for(state.writer)
                out = b""
                for ch in channels:
                    self.pubsub.unsubscribe(ch, state.writer)
                    n_ch = len(self.pubsub.channels_for(state.writer))
                    out += enc_array([enc_bulk("unsubscribe"), enc_bulk(ch), enc_int(n_ch)])
                return out

            if cmd == "PUBLISH":
                if len(args) != 3:
                    return enc_error("ERR wrong number of arguments for 'publish'")
                n = await self.pubsub.publish(args[1], args[2])
                return enc_int(n)

            # ---- replication ----
            if cmd == "REPLICAOF" or cmd == "SLAVEOF":
                if len(args) != 3:
                    return enc_error("ERR wrong number of arguments")
                if args[1].upper() == "NO" and args[2].upper() == "ONE":
                    self._stop_replicating()
                    return enc_simple("OK")
                host, port = args[1], int(args[2])
                self._start_replicating(host, port)
                return enc_simple(f"OK now replicating from {host}:{port}")

            if cmd == "PSYNC":
                # A replica is asking us (the master) for a full sync + stream.
                await self._handle_psync(state)
                return None  # response already written directly to the socket

            return enc_error(f"ERR unknown command '{cmd}'")

        except (IndexError, ValueError) as e:
            return enc_error(f"ERR {e}")

    # ------------------------------------------------------------ info text

    def _info_text(self):
        role = "slave" if self.is_replica else "master"
        lines = [
            f"role:{role}",
            f"connected_replicas:{len(self.replica_writers)}",
            f"uptime_seconds:{int(time.time() - self.start_ts)}",
            f"db_keys:{self.store.dbsize()}",
            f"evictions:{self.store.evictions}",
            f"commands_processed:{self.command_count}",
        ]
        if self.is_replica:
            lines.append(f"master:{self.master_host}:{self.master_port}")
        return "\n".join(lines)

    # ----------------------------------------------------- master-side sync

    async def _handle_psync(self, state: ClientState):
        writer = state.writer
        state.is_replica = True
        # Send the current dataset as a burst of SET commands...
        for key, value, ttl in self.store.dump():
            parts = ["SET", key, value] + (["EX", str(ttl)] if ttl else [])
            writer.write(enc_command(parts))
        await writer.drain()
        # ...then register this connection to receive all future writes live.
        self.replica_writers.append(writer)
        peer = writer.get_extra_info("peername")
        print(f"[pyredis] replica {peer} attached (full sync sent, now streaming)")

    # ---------------------------------------------------- replica-side sync

    def _start_replicating(self, host, port):
        self._stop_replicating()
        self.is_replica = True
        self.master_host, self.master_port = host, port
        self._replica_task = asyncio.create_task(self._replicate_from_master(host, port))

    def _stop_replicating(self):
        self.is_replica = False
        self.master_host = self.master_port = None
        if self._replica_task:
            self._replica_task.cancel()
            self._replica_task = None

    async def _replicate_from_master(self, host, port):
        try:
            reader, writer = await asyncio.open_connection(host, port)
        except OSError as e:
            print(f"[pyredis] could not connect to master {host}:{port}: {e}")
            self.is_replica = False
            return
        writer.write(enc_command(["PSYNC"]))
        await writer.drain()
        print(f"[pyredis] connected to master {host}:{port}, syncing...")
        try:
            while True:
                args = await read_command(reader)
                if args is None:
                    break
                self._apply_from_log(args)
                await self.aof.log_async(args)  # so a replica also survives its own restart
        except asyncio.CancelledError:
            pass
        finally:
            writer.close()
            print(f"[pyredis] replication link to {host}:{port} closed")


def main():
    parser = argparse.ArgumentParser(description="pyredis: a toy in-memory Redis-like server")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=6380)
    parser.add_argument("--maxkeys", type=int, default=10_000, help="LRU eviction ceiling")
    parser.add_argument("--aof", default="pyredis.aof", help="path to append-only log file")
    args = parser.parse_args()

    server = PyRedisServer(max_keys=args.maxkeys, aof_path=args.aof, host=args.host, port=args.port)
    try:
        asyncio.run(server.start())
    except KeyboardInterrupt:
        print("\n[pyredis] shutting down")


if __name__ == "__main__":
    main()