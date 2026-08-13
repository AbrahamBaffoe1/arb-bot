"""arb-bot entrypoint.

    uv run main.py                 # paper mode, no keys needed
    touch data/KILL                # instant halt
"""
from __future__ import annotations

import asyncio
import logging
from pathlib import Path

import uvicorn
import yaml
from dotenv import load_dotenv
from rich.logging import RichHandler

from src.dashboard import create_app
from src.engine import Engine

ROOT = Path(__file__).resolve().parent


def load_config() -> dict:
    with open(ROOT / "config" / "config.yaml") as f:
        return yaml.safe_load(f)


async def main() -> None:
    load_dotenv(ROOT / ".env")
    logging.basicConfig(level=logging.INFO, format="%(message)s",
                        datefmt="[%X]", handlers=[RichHandler(rich_tracebacks=True)])
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)

    cfg = load_config()
    if cfg["mode"] != "paper":
        raise SystemExit("live mode is intentionally not wired yet — prove the edge in paper first")

    engine = Engine(cfg)
    server = uvicorn.Server(uvicorn.Config(
        create_app(engine),
        host=cfg["dashboard"]["host"], port=cfg["dashboard"]["port"],
        log_level="warning",
    ))

    logging.getLogger("main").info(
        "dashboard: http://localhost:%s  (iPad: http://<your-mac-LAN-ip>:%s)",
        cfg["dashboard"]["port"], cfg["dashboard"]["port"])

    try:
        await asyncio.gather(engine.run(), server.serve())
    finally:
        await engine.feeds.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
