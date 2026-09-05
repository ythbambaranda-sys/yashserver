"""A multiplayer game server: UDP for state, with reliability where it matters.

This is the shape most real-time games take, and it shows why yashserver does not
paper over the difference between UDP and TCP.

* **Position updates go over plain UDP.** They are sent many times a second and
  a lost one is irrelevant, because the next one supersedes it. Retransmitting
  a stale position would be worse than dropping it.
* **Join/leave and chat go over the reliable channel.** Those must not be lost,
  so they opt in to sequence numbers, acknowledgements and retransmission.

Run the server:

    python examples/udp_game_server.py

Then run one or more clients in other terminals:

    python examples/udp_game_server.py client alice
    python examples/udp_game_server.py client bob
"""

from __future__ import annotations

import asyncio
import json
import math
import random
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import yashserver
from yashserver import ReliableUdpChannel, UdpEndpoint

TICK_HZ = 20
PORT = 9100


# ---------------------------------------------------------------------------
# server
# ---------------------------------------------------------------------------


class World:
    """Authoritative game state, keyed by endpoint rather than connection."""

    def __init__(self) -> None:
        self.players: dict[str, dict] = {}

    def join(self, endpoint: UdpEndpoint, name: str) -> None:
        self.players[endpoint.key] = {
            "name": name,
            "endpoint": endpoint,
            "x": random.uniform(0, 100),
            "y": random.uniform(0, 100),
            "last_input": time.monotonic(),
        }

    def leave(self, endpoint: UdpEndpoint) -> str | None:
        player = self.players.pop(endpoint.key, None)
        return player["name"] if player else None

    def move(self, endpoint: UdpEndpoint, dx: float, dy: float) -> None:
        player = self.players.get(endpoint.key)
        if player is None:
            return
        player["x"] = max(0.0, min(100.0, player["x"] + dx))
        player["y"] = max(0.0, min(100.0, player["y"] + dy))
        player["last_input"] = time.monotonic()

    def snapshot(self) -> dict:
        return {
            "t": round(time.monotonic(), 3),
            "players": [
                {"name": player["name"], "x": round(player["x"], 2), "y": round(player["y"], 2)}
                for player in self.players.values()
            ],
        }

    def drop_silent(self, timeout: float = 15.0) -> list[str]:
        """Time out players we have not heard from.

        UDP gives no disconnect event, so absence of input is the only signal
        that somebody has gone. This is a game rule, not a transport feature.
        """

        now = time.monotonic()
        gone = [key for key, player in self.players.items() if now - player["last_input"] > timeout]
        names = []
        for key in gone:
            player = self.players.pop(key)
            names.append(player["name"])
        return names


def build_server() -> yashserver.YUdpServer:
    server = yashserver.YUdpServer(
        host="127.0.0.1",
        port=PORT,
        # Stay under a typical path MTU so datagrams are never IP-fragmented;
        # a fragmented datagram is lost entirely if any fragment is lost.
        max_packet_size=1200,
        rate_limit_per_window=600,
        rate_limit_window_seconds=1.0,
    )
    world = World()

    # Reliable channel for the events that must not be lost.
    reliable = ReliableUdpChannel(server, retry_interval_seconds=0.1, max_retries=8)

    @server.on_datagram
    async def dispatch(datagram, srv):
        # Reliable frames are handled by the channel; everything else is a
        # plain best-effort game packet.
        if datagram.data.startswith(b"YRL"):
            await reliable.handle_datagram(datagram, srv)
            return None

        message = datagram.json(default=None)
        if not isinstance(message, dict):
            return None

        if message.get("op") == "move":
            # Unreliable on purpose: the next tick supersedes this one.
            world.move(datagram.endpoint, float(message.get("dx", 0)), float(message.get("dy", 0)))
        return None

    @reliable.on_message
    async def on_reliable(payload: bytes, endpoint: UdpEndpoint):
        message = json.loads(payload.decode("utf-8"))
        operation = message.get("op")

        if operation == "join":
            world.join(endpoint, str(message.get("name", "anon"))[:24])
            await reliable.send(endpoint, json.dumps({"op": "welcome", "you": message.get("name")}).encode())
            await announce(f"{message.get('name')} joined")
        elif operation == "chat":
            await announce(f"{message.get('name', '?')}: {message.get('text', '')}")
        elif operation == "leave":
            name = world.leave(endpoint)
            if name:
                await announce(f"{name} left")

    async def announce(text: str) -> None:
        payload = json.dumps({"op": "announce", "text": text}).encode()
        for player in list(world.players.values()):
            await reliable.send(player["endpoint"], payload)

    async def tick() -> None:
        """Broadcast world state. Dropped snapshots simply do not matter."""

        snapshot = world.snapshot()
        for player in list(world.players.values()):
            await server.send_to(player["endpoint"], snapshot)

        for name in world.drop_silent():
            await announce(f"{name} timed out")

    @server.route("stats")
    async def stats(_endpoint, _payload, srv):
        return {
            "players": len(world.players),
            "endpoints": len(srv.known_endpoints()),
            "reliable_pending": reliable.pending_count,
            "metrics": srv.metrics.snapshot()["counters"],
        }

    async def on_start() -> None:
        reliable.start()
        server.every(1.0 / TICK_HZ, tick)

    class _Boot(yashserver.ServerPlugin):
        name = "game-boot"

        async def on_startup(self, _server):
            await on_start()

    server.add_plugin(_Boot())
    return server


# ---------------------------------------------------------------------------
# client
# ---------------------------------------------------------------------------


async def run_client(name: str) -> None:
    client = yashserver.YUdpServer(host="127.0.0.1", port=0, ddosprot=False)
    reliable = ReliableUdpChannel(client, retry_interval_seconds=0.1, max_retries=8)
    server_endpoint = UdpEndpoint("127.0.0.1", PORT)

    @client.on_datagram
    async def dispatch(datagram, srv):
        if datagram.data.startswith(b"YRL"):
            await reliable.handle_datagram(datagram, srv)
            return None
        state = datagram.json(default=None)
        if isinstance(state, dict) and state.get("players"):
            names = ", ".join(f"{p['name']}@({p['x']},{p['y']})" for p in state["players"])
            print(f"\rworld: {names}    ", end="", flush=True)
        return None

    @reliable.on_message
    async def on_reliable(payload: bytes, _endpoint):
        message = json.loads(payload.decode("utf-8"))
        print(f"\n[{message.get('op')}] {message.get('text', message)}")

    await client.start()
    reliable.start()
    await reliable.send(server_endpoint, json.dumps({"op": "join", "name": name}).encode())

    angle = random.uniform(0, math.tau)
    try:
        while True:
            angle += random.uniform(-0.3, 0.3)
            await client.send_to(
                server_endpoint,
                {"op": "move", "dx": math.cos(angle), "dy": math.sin(angle)},
            )
            await asyncio.sleep(1 / TICK_HZ)
    except (KeyboardInterrupt, asyncio.CancelledError):
        await reliable.send(server_endpoint, json.dumps({"op": "leave", "name": name}).encode())
        await asyncio.sleep(0.2)
    finally:
        await client.stop()


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "client":
        player_name = sys.argv[2] if len(sys.argv) > 2 else f"player{random.randint(1, 99)}"
        try:
            asyncio.run(run_client(player_name))
        except KeyboardInterrupt:
            pass
    else:
        print(f"UDP game server on 127.0.0.1:{PORT} at {TICK_HZ} Hz")
        print("Start players with:  python examples/udp_game_server.py client <name>")
        yashserver.run_many(build_server())
