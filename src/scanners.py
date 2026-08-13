"""Opportunity scanners.

Both scanners price opportunities the honest way:
  * walk real book depth for the configured notional (not top-of-book fantasy)
  * subtract taker fees on EVERY leg
  * subtract a slippage safety buffer on every leg
An opportunity only surfaces if the edge survives all of that.
"""
from __future__ import annotations

import itertools

from .feeds import BookStore
from .models import Opportunity, walk_book


class CrossExchangeScanner:
    """Same asset, USDT-quoted on 2+ venues: buy cheap venue, sell rich venue.

    Assumes balances are pre-positioned on both venues (how real cross-venue
    arb desks work — no per-trade transfers; inventory is rebalanced later).
    """

    def __init__(self, cfg: dict, store: BookStore) -> None:
        self.cfg = cfg
        self.store = store

    def _routes(self):
        """Yield every depth-walked executable route right now:
        (group, buy_venue, buy_book, sell_venue, sell_book, buy_px, sell_px, qty, gross)."""
        stale_ms = self.cfg["scanner"]["stale_book_ms"]
        notional = self.cfg["risk"]["trade_notional_usdt"]
        for group, mapping in self.cfg["arb_groups"].items():
            live = []
            for venue, symbol in mapping.items():
                if self.store.venue_status.get(venue) != "live":
                    continue
                book = self.store.get(venue, symbol)
                if book and book.is_fresh(stale_ms) and book.bids and book.asks:
                    live.append((venue, book))
            for (va, ba), (vb, bb) in itertools.permutations(live, 2):
                # buy on va (walk asks), sell same qty into vb's bids
                buy_px, qty = walk_book(ba.asks, notional)
                if qty <= 0:
                    continue
                sell_quote = 0.0
                remaining = qty
                for price, lvl_qty in bb.bids:
                    take = min(remaining, lvl_qty)
                    sell_quote += take * price
                    remaining -= take
                    if remaining <= 1e-12:
                        break
                if remaining > 1e-12:
                    continue  # not enough depth to exit — skip, don't pretend
                sell_px = sell_quote / qty
                gross = (sell_px - buy_px) / buy_px
                yield group, va, ba, vb, bb, buy_px, sell_px, qty, gross

    def measure(self) -> list[dict]:
        """Best gross (pre-fee) executable spread per group — for the stats recorder."""
        best: dict[str, dict] = {}
        for group, va, _, vb, _, buy_px, sell_px, qty, gross in self._routes():
            if group not in best or gross > best[group]["gross"]:
                best[group] = {"group": group, "buy_venue": va, "sell_venue": vb,
                               "gross": gross, "qty": qty}
        return list(best.values())

    def scan(self) -> list[Opportunity]:
        out: list[Opportunity] = []
        buf = self.cfg["scanner"]["slippage_buffer"]
        min_edge = self.cfg["scanner"]["min_net_edge"]
        notional = self.cfg["risk"]["trade_notional_usdt"]

        for group, va, ba, vb, bb, buy_px, sell_px, qty, gross in self._routes():
            fee_a = self.cfg["venues"][va]["taker_fee"]
            fee_b = self.cfg["venues"][vb]["taker_fee"]
            net = gross - fee_a - fee_b - 2 * buf
            if net >= min_edge:
                out.append(Opportunity(
                        kind="cross", group=group,
                        detail=f"buy {va} @ {buy_px:.6g} -> sell {vb} @ {sell_px:.6g}",
                        buy_venue=va, sell_venue=vb,
                        notional=notional, gross_edge=gross, net_edge=net,
                        net_profit=net * notional,
                        legs=[
                            {"venue": va, "symbol": ba.symbol, "side": "buy",
                             "price": buy_px, "qty": qty, "fee": fee_a},
                            {"venue": vb, "symbol": bb.symbol, "side": "sell",
                             "price": sell_px, "qty": qty, "fee": fee_b},
                        ],
                    ))
        return out


class TriangularScanner:
    """Single-venue loop: USDT -> A -> B -> USDT via three books.

    Triangle spec [A/USDT, B/A, B/USDT]:
      leg1 buy A with USDT, leg2 buy B with A, leg3 sell B for USDT.
    Also evaluates the reverse loop.
    """

    def __init__(self, cfg: dict, store: BookStore) -> None:
        self.cfg = cfg
        self.store = store

    def scan(self) -> list[Opportunity]:
        out: list[Opportunity] = []
        stale_ms = self.cfg["scanner"]["stale_book_ms"]
        buf = self.cfg["scanner"]["slippage_buffer"]
        min_edge = self.cfg["scanner"]["min_net_edge"]
        notional = self.cfg["risk"]["trade_notional_usdt"]

        for venue, triangles in self.cfg.get("triangles", {}).items():
            if self.store.venue_status.get(venue) != "live":
                continue
            fee = self.cfg["venues"][venue]["taker_fee"]
            for tri in triangles:
                s1, s2, s3 = tri  # e.g. BTC/USDT, ETH/BTC, ETH/USDT
                b1, b2, b3 = (self.store.get(venue, s) for s in tri)
                if not all(b and b.is_fresh(stale_ms) and b.bids and b.asks for b in (b1, b2, b3)):
                    continue

                for direction in ("fwd", "rev"):
                    if direction == "fwd":
                        # USDT -> buy BTC (asks of s1) -> buy ETH w/ BTC (asks of s2) -> sell ETH (bids of s3)
                        px1, qty_a = walk_book(b1.asks, notional)          # BTC bought
                        if qty_a <= 0:
                            continue
                        qty_a *= (1 - fee)
                        px2, qty_b = walk_book(b2.asks, qty_a)              # ETH bought with BTC
                        if qty_b <= 0:
                            continue
                        qty_b *= (1 - fee)
                        final = self._sell_into_bids(b3.bids, qty_b)        # USDT out
                        route = f"{venue}: USDT->{s1.split('/')[0]}->{s2.split('/')[0]}->USDT"
                    else:
                        # USDT -> buy ETH (asks of s3) -> sell ETH for BTC (bids of s2) -> sell BTC (bids of s1)
                        px1, qty_b = walk_book(b3.asks, notional)           # ETH bought
                        if qty_b <= 0:
                            continue
                        qty_b *= (1 - fee)
                        qty_a = self._sell_into_bids(b2.bids, qty_b)        # BTC received
                        if qty_a <= 0:
                            continue
                        qty_a *= (1 - fee)
                        final = self._sell_into_bids(b1.bids, qty_a)        # USDT out
                        route = f"{venue}: USDT->{s2.split('/')[0]}->{s1.split('/')[0]}->USDT"
                    if final <= 0:
                        continue
                    final *= (1 - fee)
                    gross = final / notional - 1
                    net = gross - 3 * buf
                    if net >= min_edge:
                        out.append(Opportunity(
                            kind="tri", group="+".join(s.split("/")[0] for s in (s1, s2)),
                            detail=route, buy_venue=venue, sell_venue=venue,
                            notional=notional, gross_edge=gross, net_edge=net,
                            net_profit=net * notional,
                            legs=[{"venue": venue, "route": route, "direction": direction}],
                        ))
        return out

    @staticmethod
    def _sell_into_bids(bids: list[tuple[float, float]], base_qty: float) -> float:
        """Sell base_qty into bid levels, return quote received (0 if depth short)."""
        remaining = base_qty
        quote = 0.0
        for price, qty in bids:
            take = min(remaining, qty)
            quote += take * price
            remaining -= take
            if remaining <= 1e-12:
                return quote
        return 0.0
