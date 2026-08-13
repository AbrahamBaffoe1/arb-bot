"""Live order-book feeds via ccxt.pro websockets.

Public data only — no API keys required. Each (venue, symbol) gets its own
watch loop; books land in a shared BookStore read by the scanners.
A venue that fails repeatedly (e.g. Binance geo-block) disables itself
instead of taking the engine down.
"""
from __future__ import annotations

import asyncio
import logging
import time

import ccxt.pro as ccxtpro

from .models import Book

log = logging.getLogger("feeds")


class BookStore:
    """venue -> symbol -> Book, shared between feed tasks and scanners."""

    def __init__(self) -> None:
        self.books: dict[str, dict[str, Book]] = {}
        self.venue_status: dict[str, str] = {}  # connecting | live | disabled

    def get(self, venue: str, symbol: str) -> Book | None:
        return self.books.get(venue, {}).get(symbol)

    def update(self, venue: str, symbol: str, bids, asks) -> None:
        book = self.books.setdefault(venue, {}).setdefault(symbol, Book(venue, symbol))
        book.bids = [(float(p), float(q)) for p, q in bids]
        book.asks = [(float(p), float(q)) for p, q in asks]
        book.ts = time.time()


class FeedManager:
    def __init__(self, cfg: dict, store: BookStore) -> None:
        self.cfg = cfg
        self.store = store
        self.exchanges: dict[str, ccxtpro.Exchange] = {}
        self._tasks: list[asyncio.Task] = []

    def _venue_symbols(self) -> dict[str, set[str]]:
        """Collect every symbol each venue must stream (arb groups + triangles)."""
        wanted: dict[str, set[str]] = {}
        for group in self.cfg["arb_groups"].values():
            for venue, symbol in group.items():
                wanted.setdefault(venue, set()).add(symbol)
        for venue, tris in self.cfg.get("triangles", {}).items():
            for tri in tris:
                wanted.setdefault(venue, set()).update(tri)
        return wanted

    async def start(self) -> None:
        depth = self.cfg["scanner"]["book_depth"]
        for venue, symbols in self._venue_symbols().items():
            vcfg = self.cfg["venues"].get(venue, {})
            if not vcfg.get("enabled", False):
                continue
            ex = getattr(ccxtpro, venue)({"enableRateLimit": True})  # ccxt "coinbase" == Advanced Trade
            self.exchanges[venue] = ex
            self.store.venue_status[venue] = "connecting"
            for symbol in sorted(symbols):
                self._tasks.append(
                    asyncio.create_task(self._watch(venue, ex, symbol, depth), name=f"feed:{venue}:{symbol}")
                )

    async def _watch(self, venue: str, ex, symbol: str, depth: int) -> None:
        failures = 0
        while True:
            try:
                ob = await ex.watch_order_book(symbol, depth)
                self.store.update(venue, symbol, ob["bids"][:depth], ob["asks"][:depth])
                if self.store.venue_status.get(venue) != "live":
                    self.store.venue_status[venue] = "live"
                    log.info("%s live (%s)", venue, symbol)
                failures = 0
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001 — venue errors must not kill the engine
                msg = str(e)
                if "451" in msg or "restricted location" in msg:
                    if self.store.venue_status.get(venue) != "disabled":
                        self.store.venue_status[venue] = "disabled"
                        log.error("%s geo-blocked (HTTP 451) — venue disabled", venue)
                    return
                if "does not have market symbol" in msg:
                    log.warning("%s has no market %s — dropping feed", venue, symbol)
                    return
                failures += 1
                if failures == 1:
                    log.warning("%s %s feed error: %s", venue, symbol, msg[:200])
                if failures >= 8:
                    self.store.venue_status[venue] = "disabled"
                    log.error("%s disabled after repeated failures", venue)
                    return
                await asyncio.sleep(min(2**failures, 30))

    async def close(self) -> None:
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        for ex in self.exchanges.values():
            try:
                await ex.close()
            except Exception:  # noqa: BLE001
                pass
