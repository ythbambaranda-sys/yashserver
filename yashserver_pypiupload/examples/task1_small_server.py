from __future__ import annotations

import yserver


class WelcomePlugin(yserver.ServerPlugin):
    name = "welcome-plugin"

    async def on_tcp_connect(self, client, server) -> None:
        await server.send(client, {"msg": "Welcome to yserver. Try: ping, echo hello, or tools"})


app = yserver.YSyncServer(host="127.0.0.1", port=9000)
app.add_plugin(WelcomePlugin())


@app.route("ping")
def ping(_client, _payload, _server):
    return {"reply": "pong", "time": yserver.ServerTools.utc_now()}


@app.route("echo")
def echo(_client, payload, _server):
    return {"echo": payload}


@app.route("tools")
def tools(_client, _payload, server: yserver.YSyncServer):
    return {"tools": sorted(server.tools.keys())}


if __name__ == "__main__":
    print("TCP server on 127.0.0.1:9000")
    print("Connect with: nc 127.0.0.1 9000")
    try:
        app.run()
    except KeyboardInterrupt:
        print("Server stopped")
