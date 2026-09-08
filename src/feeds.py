"""Live order-book feeds via ccxt.pro websockets.

Public data only — no API keys required. Each (venue, symbol) gets its own
watch loop; books land in a shared BookStore read by the scanners.
Transient errors retry with backoff; unavailable markets are excluded.
"""
from __future__ import annotations

import asyncio
import logging
import math
import time
from collections import OrderedDict

import ccxt.pro as ccxtpro
from ccxt.base.errors import InvalidOrder

from .models import Book
from .kraken_feed import CheckedKraken

log = logging.getLogger("feeds")


def public_trade_payload(venue,trade):
    payload={k:trade.get(k) for k in ('id','timestamp','side','price','amount')}
    # Coinbase market_trades labels the maker side; Kraken labels the taker.
    # Pin this meaning in the tape rather than relying on CCXT's side field.
    if venue=='coinbase':
        raw_side=(trade.get('info') or {}).get('side',trade.get('side'))
        payload['side']={'buy':'sell','sell':'buy'}.get(str(raw_side).lower())
    payload['side_basis']='taker'
    return payload


class BookStore:
    """venue -> symbol -> Book, shared between feed tasks and scanners."""

    def __init__(self) -> None:
        self.books: dict[str, dict[str, Book]] = {}
        self.venue_status: dict[str, str] = {}  # connecting | live | disabled
        self.symbol_status: dict[str, dict[str, str]] = {}
        self.markets: dict[str, dict] = {}
        self.exchanges: dict = {}

    def get(self, venue: str, symbol: str) -> Book | None:
        return self.books.get(venue, {}).get(symbol)

    def update(self, venue: str, symbol: str, bids, asks, timestamp=None) -> None:
        book = self.books.setdefault(venue, {}).setdefault(symbol, Book(venue, symbol))
        book.bids = [(float(row[0]), float(row[1])) for row in bids]
        book.asks = [(float(row[0]), float(row[1])) for row in asks]
        book.ts = time.time()
        book.exchange_ts = float(timestamp) / 1000 if timestamp is not None else None
        book.revision += 1
        self.symbol_status.setdefault(venue, {})[symbol] = "live" if book.valid() else "invalid book"

    def usable(self, venue: str, symbol: str, stale_ms: float) -> bool:
        b = self.get(venue, symbol)
        return bool(self.symbol_status.get(venue, {}).get(symbol) == "live"
                    and b and b.valid() and b.is_fresh(stale_ms)
                    and self.markets.get(venue, {}).get(symbol))

    def amount(self, venue: str, symbol: str, qty: float) -> float:
        if not math.isfinite(qty) or qty <= 0:
            return 0.0
        try:
            return float(self.exchanges[venue].amount_to_precision(symbol, qty))
        except InvalidOrder:
            return 0.0

    def within_limits(self, venue: str, symbol: str, qty: float, cost: float) -> bool:
        market = self.markets.get(venue, {}).get(symbol)
        if not market or market.get("active") is False or not market.get("spot"):
            return False
        for key, value in (("amount", qty), ("cost", cost)):
            limits = (market.get("limits") or {}).get(key) or {}
            if limits.get("min") is not None and value < limits["min"]:
                return False
            if limits.get("max") is not None and value > limits["max"]:
                return False
        return qty > 0 and cost > 0


class FeedManager:
    def __init__(self, cfg: dict, store: BookStore, recorder=None) -> None:
        self.cfg = cfg
        self.store = store
        self.exchanges: dict[str, ccxtpro.Exchange] = {}
        self._tasks: list[asyncio.Task] = []
        self.recorder = recorder
        self.metrics = {}

    def event(self, kind, venue='', symbol='', body=None, source_ts=None):
        if self.recorder:
            self.recorder.emit(kind,venue,symbol,body,source_ts)

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
        for venue, symbols in self._venue_symbols().items():
            if self.cfg["venues"].get(venue, {}).get("enabled"):
                self._tasks.append(asyncio.create_task(self._start_venue(venue, symbols), name=f'markets:{venue}'))

    async def _start_venue(self, venue: str, symbols: set[str]) -> None:
        depth = self.cfg["scanner"]["book_depth"]
        adapter=CheckedKraken if venue=='kraken' else getattr(ccxtpro,venue)
        ex = adapter({"enableRateLimit": True, "timeout": 20000})
        if venue == 'kraken':
            ex.options.setdefault('watchOrderBook', {})['limit'] = depth
        self.exchanges[venue] = ex
        self.store.exchanges[venue] = ex
        self.store.venue_status[venue] = "connecting"
        failures = 0
        while True:
            try:
                markets = await ex.load_markets(reload=True)
                self.store.markets[venue] = markets
                self.event('markets',venue,body={'precision_mode':ex.precisionMode,
                    'markets':{s:{k:markets[s].get(k) for k in ('id','symbol','base','quote','spot','active','precision','limits')}
                               for s in symbols if s in markets}})
                for symbol in sorted(symbols):
                    market = markets.get(symbol)
                    if not market or market.get("active") is False or not market.get("spot"):
                        self.store.symbol_status.setdefault(venue, {})[symbol] = "unavailable"
                        log.warning("%s %s unavailable — skipped", venue, symbol)
                        continue
                    self.store.symbol_status.setdefault(venue, {})[symbol] = "connecting"
                    self._tasks.append(asyncio.create_task(
                        self._watch(venue, ex, symbol, depth), name=f"feed:{venue}:{symbol}"))
                    if venue=='coinbase':
                        await asyncio.sleep(.4)
                    if self.recorder and self.cfg.get('recording',{}).get('trades',True):
                        self._tasks.append(asyncio.create_task(self._trades(venue,ex,symbol),name=f'trades:{venue}:{symbol}'))
                        if venue=='coinbase':
                            await asyncio.sleep(.4)
                return
            except Exception as exc:
                msg = str(exc)
                if '451' in msg or 'restricted location' in msg:
                    self.store.venue_status[venue] = 'disabled'
                    log.error('%s unavailable from this location', venue)
                    return
                self.store.venue_status[venue] = 'retrying'
                self.event('feed_error',venue,body={'stage':'markets','type':type(exc).__name__})
                failures = min(failures + 1, 5)
                log.warning('%s market discovery failed; retrying: %s', venue, msg[:200])
                await asyncio.sleep(min(2**failures, 30))

    async def _watch(self, venue: str, ex, symbol: str, depth: int) -> None:
        failures = 0
        while True:
            try:
                ob = await ex.watch_order_book(symbol, depth)
                self.store.update(venue, symbol, ob["bids"][:depth], ob["asks"][:depth], ob.get("timestamp"))
                book = self.store.get(venue,symbol)
                self.metrics[f'{venue}:{symbol}'] = {'received':book.ts,
                    'source_age_ms':(book.ts-book.exchange_ts)*1000 if book.exchange_ts else None,
                    'revision':book.revision,'valid':book.valid()}
                if book.valid():
                    self.event('book',venue,symbol,{'bids':book.bids,'asks':book.asks,'revision':book.revision,
                                                  'nonce':ob.get('nonce')},book.exchange_ts)
                else:
                    self.event('feed_error',venue,symbol,{'type':'invalid_book'})
                if self.store.venue_status.get(venue) != "live":
                    self.store.venue_status[venue] = "live"
                    log.info("%s live (%s)", venue, symbol)
                failures = 0
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001 — venue errors must not kill the engine
                self.store.symbol_status.setdefault(venue, {})[symbol] = "reconnecting"
                self.store.books.get(venue, {}).pop(symbol, None)
                msg = str(e)
                self.event('feed_error',venue,symbol,{'stage':'book','type':type(e).__name__})
                if "451" in msg or "restricted location" in msg:
                    if self.store.venue_status.get(venue) != "disabled":
                        self.store.venue_status[venue] = "disabled"
                        log.error("%s geo-blocked (HTTP 451) — venue disabled", venue)
                    return
                if "does not have market symbol" in msg:
                    log.warning("%s has no market %s — dropping feed", venue, symbol)
                    return
                failures = min(failures + 1, 8)
                if failures == 1:
                    log.warning("%s %s feed error: %s", venue, symbol, msg[:200])
                if failures >= 8:
                    self.store.symbol_status[venue][symbol] = "retrying"
                await asyncio.sleep(min(2**failures, 30))

    async def _trades(self,venue,ex,symbol):
        seen=OrderedDict()
        failures=0
        while True:
            try:
                rows=await ex.watch_trades(symbol)
                for trade in rows:
                    key=str(trade.get('id') or (trade.get('timestamp'),trade.get('side'),trade.get('price'),trade.get('amount')))
                    if key in seen:
                        continue
                    seen[key]=None
                    if len(seen)>20000:
                        seen.popitem(last=False)
                    payload=public_trade_payload(venue,trade)
                    self.event('trade',venue,symbol,payload,trade['timestamp']/1000 if trade.get('timestamp') else None)
                failures=0
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.event('feed_error',venue,symbol,{'stage':'trades','type':type(exc).__name__})
                failures=min(failures+1,5)
                await asyncio.sleep(min(2**failures,30))

    async def close(self) -> None:
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        for ex in self.exchanges.values():
            try:
                await ex.close()
            except Exception:  # noqa: BLE001
                pass
