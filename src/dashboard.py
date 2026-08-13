"""FastAPI dashboard server — serves the iPad UI and streams engine state."""
from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse

log = logging.getLogger("dashboard")
UI = Path(__file__).resolve().parent.parent / "dashboard" / "index.html"


def create_app(engine) -> FastAPI:
    app = FastAPI(title="arb-bot")

    @app.get("/")
    async def index():
        return FileResponse(UI)

    @app.get("/api/state")
    async def state():
        return engine.snapshot()

    @app.websocket("/ws")
    async def ws(sock: WebSocket):
        await sock.accept()
        try:
            while True:
                await sock.send_json(engine.snapshot())
                await asyncio.sleep(1.0)
        except WebSocketDisconnect:
            pass
        except Exception:  # noqa: BLE001
            pass

    return app
