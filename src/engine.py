"""Engine orchestrator: feeds -> scanners -> risk -> paper executor.

Also maintains the state snapshot the dashboard streams to the iPad.
"""
from __future__ import annotations

import asyncio
import logging
import time

from .feeds import BookStore, FeedManager
from .paper import PaperExecutor
from .risk import RiskManager
from .scanners import CrossExchangeScanner, TriangularScanner

log = logging.getLogger("engine")


class Engine:
    def __init__(self, cfg: dict) -> None:
        self.cfg = cfg
        self.store = BookStore()
        self.feeds = FeedManager(cfg, self.store)
        self.cross = CrossExchangeScanner(cfg, self.store)
        self.tri = TriangularScanner(cfg, self.store)
        self.executor = PaperExecutor(cfg)
        self.risk = RiskManager(cfg)
        self.recent_opps: list[dict] = []     # last N, for dashboard
        self.equity_curve: list[list[float]] = []  # [ts, equity]
        self.started = time.time()
        self.scans = 0

    # -- live mid prices per base asset (for equity marking) --------------
    def _prices(self) -> dict[str, float]:
        prices: dict[str, float] = {}
        for group, mapping in self.cfg["arb_groups"].items():
            for venue, symbol in mapping.items():
                book = self.store.get(venue, symbol)
                if book and book.best_bid and book.best_ask:
                    prices[group] = (book.best_bid + book.best_ask) / 2
                    break
        return prices

    async def run(self) -> None:
        await self.feeds.start()
        interval = self.cfg["scanner"]["interval_ms"] / 1000
        log.info("engine running — scan every %.0fms, min net edge %.3f%%",
                 interval * 1000, self.cfg["scanner"]["min_net_edge"] * 100)
        last_equity_sample = 0.0
        last_spread_sample = 0.0
        while True:
            t0 = time.time()
            opps = self.cross.scan() + self.tri.scan()
            if t0 - last_spread_sample >= 1.0:
                self.executor.record_spreads(t0, self.cross.measure())
                last_spread_sample = t0
            self.scans += 1
            prices = self._prices()
            for opp in sorted(opps, key=lambda o: o.net_profit, reverse=True):
                self.recent_opps.insert(0, {
                    "ts": opp.ts, "kind": opp.kind, "group": opp.group,
                    "detail": opp.detail, "net_edge": opp.net_edge,
                    "net_profit": opp.net_profit,
                })
                if self.risk.allow(opp, self.executor.pnl_today(),
                                   self.executor.inventory_skew_usdt(prices)):
                    fill = self.executor.execute(opp)
                    if fill:
                        self.risk.record_fill(opp.group)
            del self.recent_opps[100:]

            if t0 - last_equity_sample >= 5 and prices:
                self.equity_curve.append([t0, round(self.executor.total_equity(prices), 2)])
                del self.equity_curve[:-2000]
                last_equity_sample = t0

            await asyncio.sleep(max(0.0, interval - (time.time() - t0)))

    # -- dashboard snapshot -------------------------------------------------
    def snapshot(self) -> dict:
        prices = self._prices()
        spreads = []
        stale_ms = self.cfg["scanner"]["stale_book_ms"]
        for group, mapping in self.cfg["arb_groups"].items():
            row = {"group": group, "venues": {}}
            for venue, symbol in mapping.items():
                book = self.store.get(venue, symbol)
                if book and book.is_fresh(stale_ms) and book.best_bid and book.best_ask:
                    row["venues"][venue] = {"bid": book.best_bid, "ask": book.best_ask}
            if len(row["venues"]) >= 2:
                best_bid = max(v["bid"] for v in row["venues"].values())
                best_ask = min(v["ask"] for v in row["venues"].values())
                row["spread"] = (best_bid - best_ask) / best_ask
            spreads.append(row)
        return {
            "ts": time.time(),
            "mode": self.cfg["mode"],
            "uptime_s": time.time() - self.started,
            "scans": self.scans,
            "venue_status": self.store.venue_status,
            "prices": prices,
            "spreads": spreads,
            "opportunities": self.recent_opps[:30],
            "fills": [{"ts": f.ts, "kind": f.kind, "detail": f.detail,
                        "notional": f.notional, "pnl": f.pnl}
                       for f in self.executor.fills[-50:]][::-1],
            "balances": self.executor.balances,
            "realized_pnl": self.executor.realized_pnl,
            "pnl_today": self.executor.pnl_today(),
            "equity": self.executor.total_equity(prices),
            "equity_curve": self.equity_curve[-500:],
            "halted": self.risk.halted_reason,
        }
