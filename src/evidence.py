"""Persistent opportunity windows and funding review based on actual observations."""
from __future__ import annotations
import json
import math
import time
from dataclasses import asdict


class Evidence:
    def __init__(self, cfg, db):
        self.cfg, self.db = cfg, db
        self.active = {}
        self.last_tick = None
        self.db.executescript('''
            CREATE TABLE IF NOT EXISTS observation_time(id INTEGER PRIMARY KEY CHECK(id=1), seconds REAL);
            INSERT OR IGNORE INTO observation_time VALUES(1,0);
            CREATE TABLE IF NOT EXISTS windows(
                id INTEGER PRIMARY KEY, route TEXT, started REAL, updated REAL,
                status TEXT, reason TEXT, plan TEXT, pnl REAL);
            CREATE INDEX IF NOT EXISTS idx_windows_started ON windows(started);
            CREATE TABLE IF NOT EXISTS fee_verifications(venue TEXT PRIMARY KEY, ts REAL, rates TEXT);
            CREATE TABLE IF NOT EXISTS open_windows(route TEXT PRIMARY KEY, window_id INTEGER, last_seen REAL);
        ''')
        # Never replay a pre-restart signal. Evidence remains, unfinished plans expire.
        with self.db:
            self.db.execute("UPDATE windows SET status='expired',reason='process restarted' WHERE status='observing'")
        # A restart must not turn one continuous dislocation into multiple fills.
        for route, window_id, last_seen in self.db.execute('SELECT * FROM open_windows'):
            self.active[route] = dict(id=window_id, first=last_seen, last=time.time(), done=True)

    def update(self, opps, healthy, now=None):
        now = time.time() if now is None else now
        if healthy and self.last_tick is not None:
            elapsed = now - self.last_tick
            if 0 <= elapsed <= 2:
                self.db.execute('UPDATE observation_time SET seconds=seconds+? WHERE id=1', (elapsed,))
        self.last_tick = now if healthy else None
        best = {}
        for o in opps:
            if o.route_key not in best or o.net_profit > best[o.route_key].net_profit:
                best[o.route_key] = o
        for key in list(self.active):
            state = self.active[key]
            if key not in best:
                if not state['done']:
                    self.result(state['id'], 'expired', 'edge disappeared before confirmation')
                    state['done'] = True
                if now - state['last'] >= self.cfg['scanner']['window_gap_s']:
                    self.db.execute('DELETE FROM open_windows WHERE route=?', (key,))
                    del self.active[key]
        ready = []
        for key, o in best.items():
            state = self.active.get(key)
            if state is None:
                cursor = self.db.execute('INSERT INTO windows(route,started,updated,status,reason,plan,pnl) VALUES(?,?,?,\'observing\',\'\',?,NULL)',
                                         (key, now, now, json.dumps(asdict(o))))
                state = dict(id=cursor.lastrowid, first=now, last=now, done=False,
                             revisions=[l['revision'] for l in o.legs], size=o.size)
                self.active[key] = state
            state['last'] = now
            self.db.execute('INSERT OR REPLACE INTO open_windows VALUES(?,?,?)', (key,state['id'],now))
            if state['done']:
                continue
            # Recheck the original size, not a newly optimized size selected after seeing the future.
            current = next((x for x in opps if x.route_key == key and x.size == state['size']), None)
            if current is None:
                self.result(state['id'], 'expired', 'original size no longer clears costs')
                state['done'] = True
                continue
            elapsed = (now - state['first']) * 1000
            changed = all(l['revision'] > old for l, old in zip(current.legs, state['revisions']))
            if elapsed >= self.cfg['scanner']['confirmation_ms'] and changed:
                state['done'] = True
                self.db.execute('UPDATE windows SET updated=?,plan=? WHERE id=?',
                                (now, json.dumps(asdict(current)), state['id']))
                ready.append((state['id'], current))
        self.db.commit()
        return ready

    def result(self, window_id, status, reason='', pnl=None):
        self.db.execute('UPDATE windows SET status=?,reason=?,pnl=? WHERE id=?', (status, reason, pnl, window_id))
        self.db.commit()

    def funding(self, halted=None, now=None):
        now = time.time() if now is None else now
        hours = self.db.execute('SELECT seconds/3600 FROM observation_time WHERE id=1').fetchone()[0]
        row = self.db.execute("SELECT count(*),coalesce(sum(pnl),0),count(DISTINCT cast(started/86400 AS INTEGER)) FROM windows WHERE status='paper_filled'").fetchone()
        total = self.db.execute('SELECT count(*) FROM windows').fetchone()[0]
        filled, pnl, days = row
        expenses = self.db.execute('SELECT coalesce(sum(amount),0) FROM expenses').fetchone()[0]
        rules = self.cfg['funding']
        blockers = []
        if hours < rules['min_observation_hours']:
            blockers.append(f"Need {rules['min_observation_hours']} hours of healthy observations; have {hours:.2f}")
        if filled < rules['min_paper_fills']:
            blockers.append(f"Need {rules['min_paper_fills']} independent paper fills; have {filled}")
        if days < rules['min_days_with_fills']:
            blockers.append(f"Need fills on {rules['min_days_with_fills']} UTC days; have {days}")
        if pnl - expenses < rules['min_net_pnl_usdt']:
            blockers.append(f"Net simulated P&L after setup costs is {pnl-expenses:.4f} USDT; requires {rules['min_net_pnl_usdt']}")
        for venue, settings in self.cfg['venues'].items():
            if settings.get('enabled'):
                receipt = self.db.execute('SELECT ts,rates FROM fee_verifications WHERE venue=?', (venue,)).fetchone()
                verified = receipt[0] if receipt else 0
                rates = json.loads(receipt[1]) if receipt else {}
                symbols = {mapping[venue] for mapping in self.cfg['arb_groups'].values() if venue in mapping}
                valid_rates = all(s in rates and isinstance(rates[s], (int,float)) and math.isfinite(rates[s])
                                  and 0 <= rates[s] <= settings['taker_fee'] for s in symbols)
                if not valid_rates or not 0 <= now - verified <= rules['fee_max_age_hours'] * 3600:
                    blockers.append(f'{venue}: verify account taker fees (verification missing or expired)')
        midnight = now // 86400 * 86400
        daily = self.db.execute('SELECT coalesce(sum(pnl),0) FROM fills WHERE ts>=?', (midnight,)).fetchone()[0]
        daily -= self.db.execute('SELECT coalesce(sum(amount),0) FROM expenses WHERE ts>=?', (midnight,)).fetchone()[0]
        daily_mark = self.db.execute('SELECT equity-initial FROM equity_marks WHERE day=?', (int(now//86400),)).fetchone()
        if min(daily, daily_mark[0] if daily_mark else daily) <= -self.cfg['risk']['max_daily_loss_usdt']:
            blockers.append('Daily paper loss limit reached')
        equity = self.db.execute('SELECT equity,ts FROM equity_marks ORDER BY ts DESC LIMIT 1').fetchone()
        initial = self.cfg['paper']['starting_balance_usdt'] * sum(v.get('enabled', False) for v in self.cfg['venues'].values())
        if not equity or now - equity[1] > rules['evidence_max_age_hours'] * 3600:
            blockers.append('No recent complete inventory valuation')
        elif equity[0] < initial:
            blockers.append('Marked paper equity is below starting capital (inventory losses included)')
        if halted:
            blockers.append(halted)
        recent = self.db.execute("SELECT max(updated) FROM windows WHERE status='paper_filled'").fetchone()[0]
        if recent is None or now - recent > rules['evidence_max_age_hours'] * 3600:
            blockers.append('No recent validated paper fill')
        selected = self.db.execute("SELECT plan FROM windows WHERE status='paper_filled' ORDER BY updated DESC LIMIT 1").fetchone()
        proposal = None
        if selected:
            plan = json.loads(selected[0])
            # A pilot funds one route, including already-owned sell inventory and a cash reserve.
            buy, sell = plan['legs'][0], plan['legs'][-1]
            capital = {}
            cash = buy['cash'] + rules['pilot_cash_reserve_usdt']
            capital[buy['venue']] = {'USDT': round(cash, 6)}
            if plan['kind'] == 'cross':
                capital.setdefault(sell['venue'], {})[sell['symbol'].split('/')[0]] = sell['qty']
                capital[sell['venue']]['USDT'] = rules['pilot_cash_reserve_usdt']
            total_quote = cash + (sell['raw_quote'] + rules['pilot_cash_reserve_usdt'] if plan['kind'] == 'cross' else 0)
            proposal = dict(route=plan['detail'], balances=capital, indicative_capital_usdt=round(total_quote, 2),
                            trade_budget_usdt=plan['size'], observed_estimated_profit_usdt=plan['net_profit'],
                            observed_at=recent, daily_loss_limit_usdt=self.cfg['risk']['max_daily_loss_usdt'])
        ready = not blockers
        return dict(status='funding_review' if ready else 'collecting_evidence', ready=ready,
                    observation_hours=hours, independent_paper_fills=filled, observed_windows=total,
                    net_paper_pnl_usdt=pnl-expenses, days_with_fills=days, blockers=blockers,
                    proposal=proposal,
                    message=('Review the proposed pilot balances in your Coinbase and Kraken accounts. '
                             'Do you want to fund this pilot and enable live trading after account preflight?'
                             if ready else 'Do not fund yet: the evidence requirements have not been met.'),
                    limitations='Paper fills are simulated. IOC legs are not atomic; partial fills can leave exposure. Inventory can lose value and need rebalancing. No profit guarantee.')
