"""
Append-Only File persistence.

Every write command (SET, DEL, EXPIRE, PERSIST, FLUSHALL) gets serialized
in the exact same RESP-array format a client would send it in, and
appended to disk. On startup we replay the file front-to-back through the
store to rebuild state after a crash or restart.

Because the log only ever grows, we also support `rewrite()` (like
Redis's BGREWRITEAOF): dump the *current* live state as a fresh, minimal
set of SET commands and atomically swap it in for the old log.
"""

import os
import asyncio
from .protocol import enc_command


class AOF:
    def __init__(self, path: str):
        self.path = path
        # line-buffered-ish: we flush + fsync after every write below,
        # since the entire point is "survives a crash".
        self._file = open(self.path, "ab")

    def log(self, parts):
        """Append one command (list of strings) to the log, fsync'd
        synchronously. Blocks the calling thread until the write hits disk.
        Kept around for contexts with no running event loop (e.g. a plain
        script); prefer log_async() inside the asyncio server."""
        self._file.write(enc_command(parts))
        self._file.flush()
        os.fsync(self._file.fileno())

    async def log_async(self, parts):
        """Same durability guarantee as log() -- the command is still
        fsync'd to disk before this returns -- but the blocking fsync()
        syscall runs in a background thread instead of on the event loop
        thread. That means other client connections keep getting served
        while this write is waiting on the disk, instead of the entire
        server freezing for the duration of the syscall."""
        self._file.write(enc_command(parts))
        self._file.flush()
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, os.fsync, self._file.fileno())

    def close(self):
        self._file.close()

    def replay(self, apply_fn):
        """Read every command in the log and call apply_fn(list_of_args) for each."""
        if not os.path.exists(self.path) or os.path.getsize(self.path) == 0:
            return 0
        with open(self.path, "rb") as f:
            data = f.read()

        pos = 0
        n = len(data)
        count = 0
        while pos < n:
            if data[pos:pos + 1] != b"*":
                raise ValueError(f"corrupt AOF at byte {pos}")
            end = data.index(b"\r\n", pos)
            argc = int(data[pos + 1:end])
            pos = end + 2
            args = []
            for _ in range(argc):
                end = data.index(b"\r\n", pos)
                length = int(data[pos + 1:end])  # skip the leading '$'
                pos = end + 2
                args.append(data[pos:pos + length].decode())
                pos += length + 2  # skip value + trailing \r\n
            apply_fn(args)
            count += 1
        return count

    def rewrite(self, dump_items):
        """Compact the log down to the minimal set of commands that
        reconstruct the current dataset. dump_items: (key, value, ttl_or_None)."""
        tmp_path = self.path + ".rewrite.tmp"
        with open(tmp_path, "wb") as f:
            for key, value, ttl in dump_items:
                if ttl is not None:
                    parts = ["SET", key, value, "EX", str(max(1, int(ttl) + 1))]
                else:
                    parts = ["SET", key, value]
                f.write(enc_command(parts))
        self._file.close()
        os.replace(tmp_path, self.path)
        self._file = open(self.path, "ab")