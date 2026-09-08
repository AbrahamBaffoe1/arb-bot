"""Persistent, cash-constrained simulation. Every balance change is settled atomically."""
from __future__ import annotations

import copy
import json
import logging
import sqlite3
import time
from pathlib import Path
from .configuration import fingerprint
from .models import Fill
from .scanners import buy_quantity, leg

log = logging.getLogger('paper')


class PaperExecutor:
    def __init__(self, cfg, db_path=None):
        self.cfg = cfg
        path = db_path or cfg.get('storage', {}).get('db_path', 'data/engine.db')
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path)
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.executescript('''
            CREATE TABLE IF NOT EXISTS fills(ts REAL, kind TEXT, grp TEXT, detail TEXT, notional REAL, pnl REAL);
            CREATE TABLE IF NOT EXISTS spreads(ts REAL, grp TEXT, buy_venue TEXT, sell_venue TEXT, gross REAL);
            CREATE INDEX IF NOT EXISTS idx_spreads_ts ON spreads(ts);
            CREATE TABLE IF NOT EXISTS paper_state(id INTEGER PRIMARY KEY CHECK(id=1), fingerprint TEXT, body TEXT);
            CREATE TABLE IF NOT EXISTS expenses(ts REAL, amount REAL, detail TEXT);
            CREATE TABLE IF NOT EXISTS equity_marks(day INTEGER PRIMARY KEY, initial REAL, equity REAL, ts REAL);
        ''')
        self.identity = fingerprint(cfg)
        state = self.db.execute('SELECT fingerprint,body FROM paper_state WHERE id=1').fetchone()
        if state and state[0] != self.identity:
            self.db.close()
            raise ValueError('Economic configuration changed. Set storage.db_path to a new database to begin a separate evaluation; the old evidence is preserved.')
        if not state and self.db.execute('SELECT count(*) FROM fills').fetchone()[0]:
            self.db.close()
            raise ValueError('Legacy fills lack persistent balances. Use a new storage.db_path.')
        start = cfg['paper']['starting_balance_usdt']
        self.balances = {v: {'USDT': float(start)} for v, c in cfg['venues'].items() if c.get('enabled')}
        self.baseline = {v: {} for v in self.balances}
        self.seeded = set()
        self.realized_pnl = 0.0
        self.last_rejection = None
        if state:
            data = json.loads(state[1])
            self.balances, self.baseline = data['balances'], data['baseline']
            self.seeded = {tuple(k) for k in data['seeded']}
            self.realized_pnl = data['realized_pnl']
        self.fills = [Fill(kind=r[1], group=r[2], detail=r[3], notional=r[4], pnl=r[5], ts=r[0])
                      for r in self.db.execute('SELECT * FROM fills ORDER BY ts DESC LIMIT 1000').fetchall()[::-1]]
        self._persist()
        self.db.commit()

    def _persist(self):
        body = json.dumps(dict(balances=self.balances, baseline=self.baseline,
                               seeded=list(self.seeded), realized_pnl=self.realized_pnl))
        self.db.execute('INSERT OR REPLACE INTO paper_state VALUES (1,?,?)', (self.identity, body))

    def seed_inventory(self, store):
        """Allocate a fixed fraction of starting cash, charging entry fees and depth."""
        for venue in self.balances:
            symbols = sorted({m[venue] for m in self.cfg['arb_groups'].values() if venue in m})
            budget = self.cfg['paper']['starting_balance_usdt'] * self.cfg['paper']['inventory_fraction'] / max(1, len(symbols))
            for symbol in symbols:
                base, quote = symbol.split('/')
                key = (venue, base)
                if quote != 'USDT' or key in self.seeded or not store.usable(venue, symbol, self.cfg['scanner']['stale_book_ms']):
                    continue
                try:
                    qty = buy_quantity(store, self.cfg, venue, symbol, budget)
                    item = leg(store, self.cfg, venue, symbol, 'buy', qty)
                except (ValueError, ArithmeticError):
                    continue
                bal = self.balances[venue]
                if not item or item['cash'] > bal['USDT']:
                    continue
                with self.db:
                    bal['USDT'] -= item['cash']
                    bal[base] = bal.get(base, 0.0) + qty
                    self.baseline[venue][base] = bal[base]
                    self.seeded.add(key)
                    expense = item['cash'] - item['raw_quote']
                    self.realized_pnl -= expense
                    self.db.execute('INSERT INTO expenses VALUES (?,?,?)', (time.time(), expense, f'seed {venue} {base}'))
                    self._persist()

    def project(self, opp):
        balances = copy.deepcopy(self.balances)
        for item in opp.legs:
            bal = balances[item['venue']]
            base, quote = item['symbol'].split('/')
            spent, received = (quote, base) if item['side'] == 'buy' else (base, quote)
            debit, credit = (item['cash'], item['qty']) if item['side'] == 'buy' else (item['qty'], item['cash'])
            if bal.get(spent, 0) + 1e-10 < debit:
                return None
            bal[spent] = max(0, bal.get(spent, 0) - debit)
            bal[received] = bal.get(received, 0) + credit
        if balances[opp.sell_venue].get('USDT', 0) < opp.reserve:
            return None
        balances[opp.sell_venue]['USDT'] -= opp.reserve
        return balances

    def execute(self, opp, prices=None):
        self.last_rejection = None
        projected = self.project(opp)
        if projected is None:
            self.last_rejection = 'insufficient pre-positioned inventory'
            return None
        if prices is not None and self.inventory_skew_usdt(prices, projected) > self.cfg['risk']['max_open_skew_usdt']:
            self.last_rejection = 'projected inventory drift exceeds limit'
            return None
        before = sum(b.get('USDT', 0) for b in self.balances.values())
        after = sum(b.get('USDT', 0) for b in projected.values())
        pnl = after - before
        if abs(pnl - opp.net_profit) > 1e-6:
            raise ValueError('Execution cash flows disagree with scanner P&L')
        fill = Fill(opp.kind, opp.group, opp.detail, opp.notional, pnl)
        with self.db:
            self.balances = projected
            self.realized_pnl += pnl
            self.db.execute('INSERT INTO fills VALUES (?,?,?,?,?,?)',
                            (fill.ts, fill.kind, fill.group, fill.detail, fill.notional, fill.pnl))
            self._persist()
        self.fills.append(fill)
        self.fills = self.fills[-1000:]
        log.info('PAPER FILL %s pnl=%+.4f USDT', opp.detail, pnl)
        return fill

    def record_spreads(self, ts, rows):
        with self.db:
            self.db.executemany('INSERT INTO spreads VALUES (?,?,?,?,?)',
                [(ts, r['group'], r['buy_venue'], r['sell_venue'], r['gross']) for r in rows])

    def inventory_skew_usdt(self, prices, balances=None):
        return max((sum(abs(qty - self.baseline.get(v, {}).get(a, 0)) * prices.get(a, 0)
                        for a, qty in assets.items() if a != 'USDT')
                    for v, assets in (balances or self.balances).items()), default=0)

    def total_equity(self, prices):
        return sum(qty if a == 'USDT' else qty * prices.get(a, 0)
                   for assets in self.balances.values() for a, qty in assets.items())

    def pnl_today(self):
        midnight = time.time() // 86400 * 86400
        pnl = self.db.execute('SELECT coalesce(sum(pnl),0) FROM fills WHERE ts>=?', (midnight,)).fetchone()[0]
        costs = self.db.execute('SELECT coalesce(sum(amount),0) FROM expenses WHERE ts>=?', (midnight,)).fetchone()[0]
        return pnl - costs

    def mark_equity(self, prices):
        now = time.time()
        day = int(now // 86400)
        equity = self.total_equity(prices)
        with self.db:
            self.db.execute('INSERT INTO equity_marks VALUES(?,?,?,?) ON CONFLICT(day) DO UPDATE SET equity=excluded.equity,ts=excluded.ts',
                            (day, equity, equity, now))
        return equity

    def risk_pnl_today(self):
        mark = self.db.execute('SELECT equity-initial FROM equity_marks WHERE day=?', (int(time.time() // 86400),)).fetchone()
        return min(self.pnl_today(), mark[0]) if mark else self.pnl_today()

    def close(self):
        self.db.close()
