"""Paper executor: simulated fills at real book prices with real fees.

Balances are tracked per venue. Cross-venue arbs assume pre-positioned
inventory on both sides (buy leg consumes USDT on venue A, sell leg
consumes base on venue B) — exactly like a real arb desk, so inventory
skew is visible and bounded by risk limits instead of hidden.
"""
from __future__ import annotations

import logging
import sqlite3
import time
from pathlib import Path

from .models import Fill, Opportunity

log = logging.getLogger("paper")


class PaperExecutor:
    def __init__(self, cfg: dict, db_path: str = "data/trades.db") -> None:
        self.cfg = cfg
        start = cfg["paper"]["starting_balance_usdt"]
        venues = [v for v, c in cfg["venues"].items() if c.get("enabled")]
        # balances[venue][asset] -> qty
        self.balances: dict[str, dict[str, float]] = {v: {"USDT": float(start)} for v in venues}
        self.fills: list[Fill] = []
        self.realized_pnl = 0.0
        self.seeded: set[tuple[str, str]] = set()

        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(db_path)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute(
            """CREATE TABLE IF NOT EXISTS fills(
                 ts REAL, kind TEXT, grp TEXT, detail TEXT,
                 notional REAL, pnl REAL)"""
        )
        self.db.execute(
            """CREATE TABLE IF NOT EXISTS spreads(
                 ts REAL, grp TEXT, buy_venue TEXT, sell_venue TEXT,
                 gross REAL)"""
        )
        self.db.execute("CREATE INDEX IF NOT EXISTS idx_spreads_ts ON spreads(ts)")
        self.db.commit()

    # -- inventory seeding -------------------------------------------------
    def seed_base(self, venue: str, asset: str, price: float) -> None:
        """First time we might SELL `asset` on `venue`, park inventory there
        (bought at current price, so seeding itself creates no fake P&L)."""
        key = (venue, asset)
        if key in self.seeded or price <= 0:
            return
        notional = self.cfg["risk"]["max_open_skew_usdt"]
        qty = notional / price
        self.balances.setdefault(venue, {}).setdefault(asset, 0.0)
        self.balances[venue][asset] += qty
        self.balances[venue]["USDT"] = self.balances[venue].get("USDT", 0.0) - notional
        self.seeded.add(key)
        log.info("seeded %s %s on %s (%.2f USDT)", f"{qty:.6g}", asset, venue, notional)

    # -- execution ---------------------------------------------------------
    def execute(self, opp: Opportunity) -> Fill | None:
        if opp.kind == "cross":
            return self._execute_cross(opp)
        return self._execute_tri(opp)

    def _execute_cross(self, opp: Opportunity) -> Fill | None:
        buy, sell = opp.legs
        base = buy["symbol"].split("/")[0]
        self.seed_base(sell["venue"], base, sell["price"])

        cost = buy["price"] * buy["qty"] * (1 + buy["fee"])
        proceeds = sell["price"] * sell["qty"] * (1 - sell["fee"])

        bal_a = self.balances[buy["venue"]]
        bal_b = self.balances[sell["venue"]]
        if bal_a.get("USDT", 0) < cost or bal_b.get(base, 0) < sell["qty"]:
            log.warning("skip %s: insufficient paper inventory", opp.group)
            return None

        bal_a["USDT"] -= cost
        bal_a[base] = bal_a.get(base, 0.0) + buy["qty"]
        bal_b[base] -= sell["qty"]
        bal_b["USDT"] = bal_b.get("USDT", 0.0) + proceeds

        pnl = proceeds - cost
        return self._record(opp, pnl)

    def _execute_tri(self, opp: Opportunity) -> Fill | None:
        venue = opp.buy_venue
        bal = self.balances[venue]
        if bal.get("USDT", 0) < opp.notional:
            return None
        # Triangle math was fully computed (with depth + fees) in the scanner;
        # settle the loop directly in USDT.
        bal["USDT"] += opp.net_profit
        return self._record(opp, opp.net_profit)

    def _record(self, opp: Opportunity, pnl: float) -> Fill:
        fill = Fill(kind=opp.kind, group=opp.group, detail=opp.detail,
                    notional=opp.notional, pnl=pnl)
        self.fills.append(fill)
        self.realized_pnl += pnl
        self.db.execute("INSERT INTO fills VALUES (?,?,?,?,?,?)",
                        (fill.ts, fill.kind, fill.group, fill.detail, fill.notional, fill.pnl))
        self.db.commit()
        log.info("FILL %s %s pnl=%+.4f USDT", fill.kind, fill.detail, fill.pnl)
        return fill

    # -- spread stats (fuel for report.py fee-scenario analysis) ------------
    def record_spreads(self, ts: float, rows: list[dict]) -> None:
        if not rows:
            return
        self.db.executemany(
            "INSERT INTO spreads VALUES (?,?,?,?,?)",
            [(ts, r["group"], r["buy_venue"], r["sell_venue"], r["gross"]) for r in rows],
        )
        self.db.commit()

    # -- reporting ----------------------------------------------------------
    def inventory_skew_usdt(self, prices: dict[str, float]) -> float:
        """Worst per-venue deviation of base holdings from seed level, in USDT."""
        worst = 0.0
        for venue, assets in self.balances.items():
            for asset, qty in assets.items():
                if asset == "USDT":
                    continue
                seeded_notional = self.cfg["risk"]["max_open_skew_usdt"] if (venue, asset) in self.seeded else 0.0
                px = prices.get(asset, 0.0)
                drift = abs(qty * px - seeded_notional)
                worst = max(worst, drift)
        return worst

    def total_equity(self, prices: dict[str, float]) -> float:
        total = 0.0
        for assets in self.balances.values():
            for asset, qty in assets.items():
                total += qty if asset == "USDT" else qty * prices.get(asset, 0.0)
        return total

    def pnl_today(self) -> float:
        midnight = time.time() - (time.time() % 86400)
        return sum(f.pnl for f in self.fills if f.ts >= midnight)
