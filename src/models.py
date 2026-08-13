"""Shared datatypes for the arb engine."""
from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass
class Book:
    """Cached order book (top-N levels) for one symbol on one venue."""
    venue: str
    symbol: str
    bids: list[tuple[float, float]] = field(default_factory=list)  # (price, qty) desc
    asks: list[tuple[float, float]] = field(default_factory=list)  # (price, qty) asc
    ts: float = 0.0

    @property
    def best_bid(self) -> float | None:
        return self.bids[0][0] if self.bids else None

    @property
    def best_ask(self) -> float | None:
        return self.asks[0][0] if self.asks else None

    def is_fresh(self, max_age_ms: float) -> bool:
        return self.ts > 0 and (time.time() - self.ts) * 1000 <= max_age_ms


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
    remaining = quote_amount
    base_total = 0.0
    quote_spent = 0.0
    for price, qty in levels:
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
