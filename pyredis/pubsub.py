"""
Pub/Sub: channel name -> set of subscribed client writers.

Nothing fancy: PUBLISH pushes a RESP ["message", channel, payload] array
straight into every subscriber's socket.
"""

from .protocol import enc_bulk, enc_array


class PubSub:
    def __init__(self):
        self.channels: dict[str, set] = {}

    def subscribe(self, channel: str, writer):
        self.channels.setdefault(channel, set()).add(writer)

    def unsubscribe(self, channel: str, writer):
        if channel in self.channels:
            self.channels[channel].discard(writer)
            if not self.channels[channel]:
                del self.channels[channel]

    def unsubscribe_all(self, writer):
        for channel in list(self.channels.keys()):
            self.unsubscribe(channel, writer)

    def channels_for(self, writer):
        return [ch for ch, subs in self.channels.items() if writer in subs]

    async def publish(self, channel: str, message: str) -> int:
        subs = list(self.channels.get(channel, set()))
        payload = enc_array([enc_bulk("message"), enc_bulk(channel), enc_bulk(message)])
        delivered = 0
        for writer in subs:
            try:
                writer.write(payload)
                await writer.drain()
                delivered += 1
            except (ConnectionError, OSError):
                # dead socket; let the connection handler clean it up
                pass
        return delivered
