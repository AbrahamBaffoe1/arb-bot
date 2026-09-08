"""Shared datatypes for the arb engine."""
from __future__ import annotations

import time
import math
from dataclasses import dataclass, field


def normalized_depth(book):
    """Strip optional per-level metadata (Kraken REST includes timestamps)."""
    return {**book, **{side:[[float(row[0]),float(row[1])] for row in book[side]]
                       for side in ('bids','asks')}}


@dataclass
class Book:
    """Cached order book (top-N levels) for one symbol on one venue."""
    venue: str
    symbol: str
    bids: list[tuple[float, float]] = field(default_factory=list)  # (price, qty) desc
    asks: list[tuple[float, float]] = field(default_factory=list)  # (price, qty) asc
    ts: float = 0.0
    exchange_ts: float | None = None
    revision: int = 0

    @property
    def best_bid(self) -> float | None:
        return self.bids[0][0] if self.bids else None

    @property
    def best_ask(self) -> float | None:
        return self.asks[0][0] if self.asks else None

    def is_fresh(self, max_age_ms: float) -> bool:
        now = time.time()
        stamps = [self.ts] + ([self.exchange_ts] if self.exchange_ts is not None else [])
        return all(t > 0 and -100 <= (now - t) * 1000 <= max_age_ms for t in stamps)

    def valid(self) -> bool:
        return (bool(self.bids and self.asks)
                and all(math.isfinite(p) and math.isfinite(q) and p > 0 and q > 0
                        for p, q in self.bids + self.asks)
                and self.bids == sorted(self.bids, reverse=True)
                and self.asks == sorted(self.asks)
                and self.best_bid < self.best_ask)


@dataclass
class Opportunity:
    """A detected arb, priced AFTER fees and slippage buffer."""
    kind: str                 # "cross" | "tri"
    group: str                # arb group ("BTC") or triangle label
    detail: str               # human-readable route
    buy_venue: str
    sell_venue: str
    notional: float           # quote (USDT) size actually fillable
    gross_edge: float         # fractional, before costs
    net_edge: float           # fractional, after fees + slippage buffer
    net_profit: float         # USDT, net_edge * notional
    ts: float = field(default_factory=time.time)
    legs: list[dict] = field(default_factory=list)  # execution plan
    reserve: float = 0.0       # estimated rebalancing cost, charged in paper
    size: float = 0.0          # input budget, stable across revalidation

    @property
    def route_key(self) -> str:
        return "|".join([self.kind, self.group] + [
            f"{leg['venue']}:{leg['symbol']}:{leg['side']}" for leg in self.legs])


@dataclass
class Fill:
    """A completed (paper or live) arb round trip."""
    kind: str
    group: str
    detail: str
    notional: float
    pnl: float                # realized USDT after fees
    ts: float = field(default_factory=time.time)


def walk_book(levels: list[tuple[float, float]], quote_amount: float) -> tuple[float, float]:
    """Walk order-book levels to spend/receive `quote_amount` of quote currency.

    Returns (avg_price, base_qty_filled). Fills only what depth allows —
    this is what makes the paper P&L honest instead of top-of-book fantasy.
    """
    if not math.isfinite(quote_amount) or quote_amount <= 0:
        return 0.0, 0.0
    remaining = quote_amount
    base_total = 0.0
    quote_spent = 0.0
    for price, qty in levels:
        if not all(math.isfinite(x) and x > 0 for x in (price, qty)):
            return 0.0, 0.0
        level_quote = price * qty
        take = min(remaining, level_quote)
        base_total += take / price
        quote_spent += take
        remaining -= take
        if remaining <= 1e-9:
            break
    if base_total <= 0:
        return 0.0, 0.0
    return quote_spent / base_total, base_total


def quote_for_base(levels: list[tuple[float, float]], qty: float) -> float | None:
    """Full fill only; an unfilled exit must never be booked as profit."""
    remaining, quote = qty, 0.0
    if not math.isfinite(qty) or qty <= 0:
        return None
    for price, available in levels:
        take = min(remaining, available)
        quote += take * price
        remaining -= take
        if remaining <= max(1e-12, qty * 1e-10):
            return quote
    return None
