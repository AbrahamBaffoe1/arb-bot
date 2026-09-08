"""Depth-priced spot routes with exchange precision, limits and explicit cash flows."""
from __future__ import annotations

import itertools
from .feeds import BookStore
from .models import Opportunity, quote_for_base, walk_book


def sizes(cfg):
    cap = cfg['risk']['trade_notional_usdt']
    return sorted(set([cap] + [s for s in cfg['scanner'].get('sizes_usdt', []) if 0 < s <= cap]))


def leg(store, cfg, venue, symbol, side, qty):
    book = store.get(venue, symbol)
    raw = quote_for_base(book.asks if side == 'buy' else book.bids, qty)
    if raw is None or not store.within_limits(venue, symbol, qty, raw):
        return None
    fee = cfg['venues'][venue]['taker_fee']
    buf = cfg['scanner']['slippage_buffer']
    cash = raw * (1 + buf) * (1 + fee) if side == 'buy' else raw * (1 - buf) * (1 - fee)
    remaining, worst = qty, 0.0
    for price, available in (book.asks if side == 'buy' else book.bids):
        worst = price
        remaining -= min(remaining, available)
        if remaining <= max(1e-12, qty * 1e-10):
            break
    return dict(venue=venue, symbol=symbol, side=side, qty=qty, price=raw / qty,
                fee=fee, cash=cash, raw_quote=raw, revision=book.revision,
                book_ts=book.ts, limit_price=worst * (1 + buf if side == 'buy' else 1 - buf))


def buy_quantity(store, cfg, venue, symbol, budget):
    fee = cfg['venues'][venue]['taker_fee']
    buf = cfg['scanner']['slippage_buffer']
    raw_budget = budget / ((1 + fee) * (1 + buf))
    px, qty = walk_book(store.get(venue, symbol).asks, raw_budget)
    if px * qty < raw_budget * (1 - 1e-9):
        return 0.0
    return store.amount(venue, symbol, qty)


class CrossExchangeScanner:
    def __init__(self, cfg: dict, store: BookStore) -> None:
        self.cfg, self.store = cfg, store

    def candidates(self) -> list[Opportunity]:
        out = []
        stale = self.cfg['scanner']['stale_book_ms']
        for group, mapping in self.cfg['arb_groups'].items():
            live = [(v, s) for v, s in mapping.items() if self.store.usable(v, s, stale)]
            for (va, sa), (vb, sb) in itertools.permutations(live, 2):
                # No implicit conversion between USD, USDC and USDT.
                if sa != sb or sa.split('/')[1] != 'USDT':
                    continue
                ba, bb = self.store.get(va, sa), self.store.get(vb, sb)
                if abs(ba.ts - bb.ts) * 1000 > self.cfg['scanner'].get('max_book_skew_ms', 1000):
                    continue
                for budget in sizes(self.cfg):
                    try:
                        qty = buy_quantity(self.store, self.cfg, va, sa, budget)
                        # Both venues must represent exactly the same base quantity.
                        for _ in range(4):
                            rounded = min(self.store.amount(va, sa, qty), self.store.amount(vb, sb, qty))
                            if rounded == qty:
                                break
                            qty = rounded
                        if qty <= 0 or any(self.store.amount(v, s, qty) != qty for v, s in ((va, sa), (vb, sb))):
                            continue
                    except (ValueError, ArithmeticError):
                        continue
                    buy = leg(self.store, self.cfg, va, sa, 'buy', qty)
                    sell = leg(self.store, self.cfg, vb, sb, 'sell', qty)
                    if not buy or not sell:
                        continue
                    reserve = buy['cash'] * self.cfg['scanner'].get('rebalance_buffer', 0.001)
                    profit = sell['cash'] - buy['cash'] - reserve
                    out.append(Opportunity(
                        kind='cross', group=group, detail=f'buy {va} {sa} -> sell {vb} {sb}',
                        buy_venue=va, sell_venue=vb, notional=buy['cash'],
                        gross_edge=sell['raw_quote'] / buy['raw_quote'] - 1,
                        net_edge=profit / buy['cash'], net_profit=profit,
                        legs=[buy, sell], reserve=reserve, size=budget))
        return out

    def scan(self):
        return [o for o in self.candidates() if self.qualifies(o)]

    def qualifies(self, o):
        return (o.net_edge >= self.cfg['scanner']['min_net_edge']
                and o.net_profit >= self.cfg['scanner'].get('min_profit_usdt', 0.10))

    def measure(self):
        best = {}
        for o in self.candidates():
            if o.group not in best or o.net_profit > best[o.group]['net_profit']:
                best[o.group] = dict(group=o.group, buy_venue=o.buy_venue, sell_venue=o.sell_venue,
                                     gross=o.gross_edge, net_edge=o.net_edge, net_profit=o.net_profit,
                                     notional=o.notional, qty=o.legs[0]['qty'])
        return list(best.values())


class TriangularScanner:
    """Three explicitly priced legs; intermediate rounding dust is not counted as profit."""
    def __init__(self, cfg, store):
        self.cfg, self.store = cfg, store

    def scan(self):
        out = []
        if not self.cfg['scanner'].get('triangular_enabled', False):
            return out
        stale = self.cfg['scanner']['stale_book_ms']
        for venue, triangles in self.cfg.get('triangles', {}).items():
            for s1, s2, s3 in triangles:
                if not all(self.store.usable(venue, s, stale) for s in (s1, s2, s3)):
                    continue
                a, quote = s1.split('/')
                b, middle = s2.split('/')
                if quote != 'USDT' or middle != a or s3 != f'{b}/{quote}':
                    continue
                stamps = [self.store.get(venue, s).ts for s in (s1, s2, s3)]
                if (max(stamps) - min(stamps)) * 1000 > self.cfg['scanner'].get('max_book_skew_ms', 1000):
                    continue
                for route in ([(s1, 'buy'), (s2, 'buy'), (s3, 'sell')],
                              [(s3, 'buy'), (s2, 'sell'), (s1, 'sell')]):
                    for budget in sizes(self.cfg):
                        amount, legs = budget, []
                        for symbol, side in route:
                            try:
                                qty = (buy_quantity(self.store, self.cfg, venue, symbol, amount)
                                       if side == 'buy' else self.store.amount(venue, symbol, amount))
                            except (ValueError, ArithmeticError):
                                break
                            item = leg(self.store, self.cfg, venue, symbol, side, qty)
                            if item is None:
                                break
                            legs.append(item)
                            amount = qty if side == 'buy' else item['cash']
                        if len(legs) != 3:
                            continue
                        cost = legs[0]['cash']
                        profit = amount - cost
                        if (profit / cost < self.cfg['scanner']['min_net_edge']
                                or profit < self.cfg['scanner'].get('min_profit_usdt', 0.10)):
                            continue
                        raw_factor = 1.0
                        for item in legs:
                            raw_factor *= 1 / item['price'] if item['side'] == 'buy' else item['price']
                        out.append(Opportunity('tri', f'{a}+{b}',
                            f"{venue}: " + ' -> '.join(f'{side} {s}' for s, side in route),
                            venue, venue, cost, raw_factor - 1, profit / cost, profit,
                            legs=legs, size=budget))
        return out
