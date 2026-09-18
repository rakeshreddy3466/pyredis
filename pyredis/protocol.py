"""
RESP (REdis Serialization Protocol) encoder/decoder.

This is the same wire protocol real Redis speaks, which means our server
can be tested with the real `redis-cli` binary if you have one installed,
and our own client (cli.py) speaks proper RESP too instead of some
made-up line format.

Reference: https://redis.io/docs/latest/develop/reference/protocol-spec/
"""

import asyncio


# ---------- Encoding (server/client -> wire bytes) ----------

def enc_simple(s: str) -> bytes:
    """+OK\r\n style simple string"""
    return f"+{s}\r\n".encode()


def enc_error(s: str) -> bytes:
    """-ERR message\r\n style error"""
    return f"-{s}\r\n".encode()


def enc_int(n: int) -> bytes:
    return f":{n}\r\n".encode()


def enc_bulk(s) -> bytes:
    """Bulk string. None encodes to the RESP null bulk string ($-1)."""
    if s is None:
        return b"$-1\r\n"
    if isinstance(s, bytes):
        b = s
    else:
        b = str(s).encode()
    return b"$" + str(len(b)).encode() + b"\r\n" + b + b"\r\n"


def enc_array(items) -> bytes:
    """items is a list of *already encoded* RESP values, or None for null array."""
    if items is None:
        return b"*-1\r\n"
    out = b"*" + str(len(items)).encode() + b"\r\n"
    for it in items:
        out += it
    return out


def enc_command(parts) -> bytes:
    """Encode a command (list of strings) the way a client sends it to a server:
    an array of bulk strings. Also used to serialize commands into the AOF file
    and to stream commands to replicas."""
    return enc_array([enc_bulk(p) for p in parts])


# ---------- Decoding (wire bytes -> python) ----------

class ProtocolError(Exception):
    pass


async def read_command(reader: asyncio.StreamReader):
    """Reads one client command from the stream.

    Supports the standard RESP array-of-bulk-strings format that real
    clients (including redis-cli) send, and also a simple 'inline command'
    fallback (plain text line, space separated) so you can debug with
    something as dumb as `nc localhost 6380`.

    Returns a list of strings, or None on clean EOF.
    """
    line = await reader.readline()
    if line == b"":
        return None  # EOF
    line = line.rstrip(b"\r\n")
    if not line:
        return []

    if line[0:1] == b"*":
        try:
            count = int(line[1:])
        except ValueError:
            raise ProtocolError("invalid multibulk length")
        if count <= 0:
            return []
        args = []
        for _ in range(count):
            type_line = await reader.readline()
            type_line = type_line.rstrip(b"\r\n")
            if type_line[0:1] != b"$":
                raise ProtocolError("expected bulk string")
            length = int(type_line[1:])
            if length == -1:
                args.append(None)
                continue
            data = await reader.readexactly(length)
            await reader.readexactly(2)  # trailing \r\n
            args.append(data.decode(errors="replace"))
        return args
    else:
        # inline command, e.g. typed over telnet/nc
        return line.decode(errors="replace").split()
