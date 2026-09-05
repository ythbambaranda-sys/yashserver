"""A chat server: WebSocket rooms served on the same port as its web page.

Open http://127.0.0.1:8082 in two browser tabs, join the same room, and type.

    python examples/websocket_chat_rooms.py

Because the WebSocket is mounted on the HTTP server, the page and the socket
share one origin and (if you enable TLS) one certificate.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import yashserver
from yashserver import CloseCode, ServerTools

PORT = 8082

app = yashserver.YHttpServer(host="127.0.0.1", port=PORT, ddosprot=True)
chat = yashserver.YWebSocketServer(
    host="127.0.0.1",
    port=9082,
    # Chat is chatty; allow a healthy burst before rate limiting kicks in.
    rate_limit_per_window=120,
    rate_limit_window_seconds=10.0,
    ping_interval_seconds=20.0,
)

NAMES: dict[str, str] = {}


@chat.route("/ws")
async def socket(session, message, server):
    if isinstance(message, bytes):
        return {"type": "error", "text": "this room is text only"}

    payload = ServerTools.from_json(message, default=None)
    if not isinstance(payload, dict):
        return {"type": "error", "text": "expected JSON"}

    action = payload.get("action")

    if action == "join":
        name = str(payload.get("name") or "anon")[:24]
        room = str(payload.get("room") or "lobby")[:32]

        for previous in list(session.rooms):
            server.leave_room(session, previous)

        NAMES[session.id] = name
        server.join_room(session, room)
        await server.broadcast_to_room(
            room,
            {"type": "system", "text": f"{name} joined"},
            exclude=session.id,
        )
        return {
            "type": "joined",
            "room": room,
            "members": [NAMES.get(member.id, "?") for member in server.room_members(room)],
        }

    if action == "say":
        if not session.rooms:
            return {"type": "error", "text": "join a room first"}
        room = next(iter(session.rooms))
        text = str(payload.get("text") or "")[:2000]
        delivered = await server.broadcast_to_room(
            room,
            {"type": "chat", "from": NAMES.get(session.id, "?"), "text": text},
            exclude=session.id,
        )
        return {"type": "sent", "delivered": delivered}

    if action == "rooms":
        return {"type": "rooms", "rooms": server.rooms()}

    if action == "bye":
        await session.close(CloseCode.NORMAL, "goodbye")
        return None

    return {"type": "error", "text": f"unknown action {action!r}"}


class Presence(yashserver.ServerPlugin):
    """Announce departures using the disconnect hook."""

    name = "presence"

    async def on_ws_disconnect(self, session, server):
        name = NAMES.pop(session.id, None)
        for room in list(session.rooms):
            await server.broadcast_to_room(room, {"type": "system", "text": f"{name or 'someone'} left"})


chat.add_plugin(Presence())
app.mount_websocket(chat, "/ws")


PAGE = """<!doctype html>
<title>yashserver chat</title>
<style>
 body{font:14px system-ui;margin:0;display:flex;flex-direction:column;height:100vh}
 #log{flex:1;overflow:auto;padding:12px;background:#fafafa}
 .system{color:#888;font-style:italic}
 .me{color:#0a7}
 form{display:flex;gap:6px;padding:10px;border-top:1px solid #ddd}
 input{padding:8px;font:inherit} #text{flex:1}
</style>
<div id=log></div>
<form id=join><input id=name placeholder=name value=guest><input id=room placeholder=room value=lobby>
<button>join</button></form>
<form id=send><input id=text placeholder="message" autocomplete=off><button>send</button></form>
<script>
const log = document.getElementById('log');
const add = (text, cls='') => {
  const div = document.createElement('div');
  div.className = cls; div.textContent = text; log.append(div); log.scrollTop = log.scrollHeight;
};
const scheme = location.protocol === 'https:' ? 'wss' : 'ws';
const ws = new WebSocket(`${scheme}://${location.host}/ws`);
ws.onopen = () => add('connected', 'system');
ws.onclose = e => add('disconnected (' + e.code + ')', 'system');
ws.onmessage = e => {
  const m = JSON.parse(e.data);
  if (m.type === 'chat') add(m.from + ': ' + m.text);
  else if (m.type === 'system') add(m.text, 'system');
  else if (m.type === 'joined') add('joined ' + m.room + ' with ' + m.members.join(', '), 'system');
  else if (m.type === 'error') add('error: ' + m.text, 'system');
};
join.onsubmit = e => { e.preventDefault();
  ws.send(JSON.stringify({action:'join', name:name.value, room:room.value})); };
send.onsubmit = e => { e.preventDefault();
  if (!text.value) return;
  ws.send(JSON.stringify({action:'say', text:text.value}));
  add('you: ' + text.value, 'me'); text.value = ''; };
</script>
"""


@app.get("/")
async def index(_request, _server):
    return yashserver.HttpResponse.html_response(PAGE)


@app.get("/rooms")
async def rooms(_request, _server):
    return {"rooms": chat.rooms(), "connections": len(chat.clients)}


if __name__ == "__main__":
    print(f"Chat on http://127.0.0.1:{PORT}  (WebSocket at /ws on the same port)")
    yashserver.run_many(app, chat)
