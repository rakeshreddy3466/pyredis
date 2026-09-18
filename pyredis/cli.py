"""
pyredis-cli: a small interactive client, à la redis-cli.

Usage:
    python -m pyredis.cli --port 6380              # interactive REPL
    python -m pyredis.cli --port 6380 GET foo       # one-shot command
"""

import argparse
import asyncio
import shlex
import sys

from .protocol import enc_command


async def read_reply(reader: asyncio.StreamReader):
    """Decode one RESP reply into a python value for pretty-printing."""
    line = await reader.readline()
    if not line:
        return None
    prefix, rest = line[0:1], line[1:].rstrip(b"\r\n")

    if prefix == b"+":
        return rest.decode()
    if prefix == b"-":
        return Exception(rest.decode())
    if prefix == b":":
        return int(rest)
    if prefix == b"$":
        length = int(rest)
        if length == -1:
            return None
        data = await reader.readexactly(length)
        await reader.readexactly(2)
        return data.decode(errors="replace")
    if prefix == b"*":
        count = int(rest)
        if count == -1:
            return None
        return [await read_reply(reader) for _ in range(count)]
    raise ValueError(f"unknown RESP type byte: {prefix!r}")


def format_reply(value, indent=0) -> str:
    if isinstance(value, Exception):
        return f"(error) {value}"
    if value is None:
        return "(nil)"
    if isinstance(value, int):
        return f"(integer) {value}"
    if isinstance(value, list):
        if not value:
            return "(empty array)"
        return "\n".join(f"{i + 1}) {format_reply(v)}" for i, v in enumerate(value))
    return f"\"{value}\""


async def repl(host, port, one_shot_args):
    reader, writer = await asyncio.open_connection(host, port)
    prompt = f"{host}:{port}> "

    async def send(parts):
        writer.write(enc_command(parts))
        await writer.drain()
        return await read_reply(reader)

    if one_shot_args:
        reply = await send(one_shot_args)
        print(format_reply(reply))
        if one_shot_args[0].upper() == "SUBSCRIBE":
            # Real redis-cli blocks here printing pushed messages forever;
            # do the same instead of closing right after the ack.
            print("(reading messages... Ctrl-C to stop)")
            try:
                while True:
                    msg = await read_reply(reader)
                    if msg is None:
                        break
                    print(format_reply(msg))
            except (asyncio.CancelledError, KeyboardInterrupt):
                pass
        writer.close()
        return

    print(f"pyredis-cli connected to {host}:{port}. Type QUIT or Ctrl-D to exit.")
    loop = asyncio.get_event_loop()
    while True:
        try:
            line = await loop.run_in_executor(None, input, prompt)
        except EOFError:
            print()
            break
        line = line.strip()
        if not line:
            continue
        parts = shlex.split(line)
        if parts[0].upper() == "QUIT":
            break
        reply = await send(parts)
        print(format_reply(reply))
        if parts[0].upper() == "SUBSCRIBE":
            # keep listening for pushed messages instead of returning to the prompt
            print("(reading messages... Ctrl-C to stop)")
            try:
                while True:
                    msg = await read_reply(reader)
                    print(format_reply(msg))
            except (asyncio.CancelledError, KeyboardInterrupt):
                pass

    writer.close()


def main():
    parser = argparse.ArgumentParser(description="pyredis-cli: talk to a pyredis server")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6380)
    parser.add_argument("command", nargs="*", help="run a single command and exit")
    args = parser.parse_args()

    try:
        asyncio.run(repl(args.host, args.port, args.command))
    except (ConnectionRefusedError, OSError) as e:
        print(f"Could not connect to {args.host}:{args.port}: {e}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
