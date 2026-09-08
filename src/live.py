"""Coinbase/Kraken IOC execution. Unknown or unmatched fills latch a persistent halt."""
from __future__ import annotations
import asyncio
import json
import math
import os
import time
import uuid
import ccxt.pro as ccxt
from dataclasses import asdict
from .models import Fill
from .paper import PaperExecutor
from .recovery import OrderLedger,DisconnectGuard
from .portfolio import PortfolioRisk


class LiveExecutor(PaperExecutor):
    def __init__(self, cfg):
        super().__init__(cfg, cfg['storage']['live_db_path'])
        self.exchanges = {}
        self.ledger=OrderLedger(self.db)
        self.portfolio=PortfolioRisk(cfg,self.db)
        self._private_tasks=[]
        self.private_status={}
        self._stream_orders={}
        self._order_events={}
        self.private_enabled=False
        self.telemetry=None
        self.disconnect_guard=None
        self.halted_reason = None
        self.db.executescript('''
            CREATE TABLE IF NOT EXISTS live_orders(client_id TEXT PRIMARY KEY, ts REAL,
                venue TEXT, symbol TEXT, side TEXT, qty REAL, limit_price REAL,
                order_id TEXT, status TEXT, result TEXT);
            CREATE TABLE IF NOT EXISTS live_halt(id INTEGER PRIMARY KEY CHECK(id=1), reason TEXT);
            CREATE TABLE IF NOT EXISTS live_initial(id INTEGER PRIMARY KEY CHECK(id=1), balances TEXT);
        ''')
        halt = self.db.execute('SELECT reason FROM live_halt WHERE id=1').fetchone()
        if halt:
            self.halted_reason = halt[0]
        if self.db.execute("SELECT count(*) FROM live_orders WHERE status!='settled'").fetchone()[0]:
            self.halt('Unsettled order journal: reconcile exchange orders before any further trading')

    def halt(self, reason):
        self.halted_reason = reason
        with self.db:
            self.db.execute('INSERT OR REPLACE INTO live_halt VALUES(1,?)', (reason,))

    async def initialize(self, establish_baseline=True, recovery=False):
        if self.halted_reason and not recovery:
            raise RuntimeError(self.halted_reason)
        for venue, settings in self.cfg['venues'].items():
            if not settings.get('enabled'):
                continue
            if venue not in ('coinbase', 'kraken'):
                raise ValueError('Live adapter supports Coinbase and Kraken only')
            key, secret = os.getenv(f'{venue.upper()}_API_KEY'), os.getenv(f'{venue.upper()}_SECRET')
            if not key or not secret:
                raise ValueError(f'{venue}: missing API credentials in .env')
            ex = getattr(ccxt, venue)({'apiKey': key, 'secret': secret.replace('\\n', '\n'),
                                        'enableRateLimit': True, 'timeout': 10000})
            self.exchanges[venue] = ex
            await ex.load_markets()
            if not recovery and not ex.features.get('spot', {}).get('createOrder', {}).get('timeInForce', {}).get('IOC'):
                raise ValueError(f'{venue}: IOC support could not be verified')
        if recovery:
            # Recovery must remain available when fees increase or order entry
            # capability changes. Neither condition makes an unknown order safe.
            await self.refresh_balances()
            return {}
        fees = await self.verify_fees()
        await self.refresh_balances()
        if not establish_baseline:
            return fees
        initial = self.db.execute('SELECT balances FROM live_initial WHERE id=1').fetchone()
        if initial:
            self.baseline = json.loads(initial[0])
        else:
            self.baseline = {v: {a: q for a, q in b.items() if a != 'USDT'} for v, b in self.balances.items()}
            with self.db:
                self.db.execute('INSERT INTO live_initial VALUES(1,?)', (json.dumps(self.baseline),))
                self._persist()
        return fees

    def start_private_streams(self):
        self.private_enabled=True
        for venue,ex in self.exchanges.items():
            self.private_status[venue]='connecting'
            self._private_tasks.append(asyncio.create_task(self._watch_orders(venue,ex),name=f'orders:{venue}'))

    async def start_disconnect_guard(self):
        if not self.cfg['live'].get('disconnect_guard_enabled',True):
            raise ValueError('Live execution requires Kraken disconnect cancellation')
        self.disconnect_guard=DisconnectGuard(self.exchanges['kraken'],self.halt,
            self.cfg['risk']['kill_switch_file'],
            lambda: not self.halted_reason and all(s=='connected' for s in self.private_status.values()))
        await self.disconnect_guard.arm()
        self._private_tasks.append(asyncio.create_task(self.disconnect_guard.run(),name='disconnect-guard'))

    async def _watch_orders(self,venue,ex):
        while True:
            try:
                orders=await ex.watch_orders()
                self.private_status[venue]='connected'
                for order in orders:
                    client=order.get('clientOrderId')
                    if not client and order.get('id'):
                        row=self.db.execute('SELECT client_id FROM live_orders WHERE venue=? AND order_id=?',(venue,order['id'])).fetchone()
                        client=row[0] if row else None
                    if client and self.db.execute('SELECT 1 FROM live_orders WHERE client_id=?',(client,)).fetchone():
                        self.ledger.observe(client,order,'user_websocket')
                        self._stream_orders[client]=order
                        self._order_events.setdefault(client,asyncio.Event()).set()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.private_status[venue]='disconnected'
                self.halt(f'{venue} private order stream failed; reconciliation required')
                return

    async def verify_fees(self):
        results = {}
        for venue, ex in self.exchanges.items():
            symbols = sorted({m[venue] for m in self.cfg['arb_groups'].values() if venue in m and m[venue] in ex.markets})
            results[venue] = {}
            all_fees = await ex.fetch_trading_fees() if ex.has.get('fetchTradingFees') is True else None
            for symbol in symbols:
                fee = (all_fees.get(symbol) if all_fees is not None else await ex.fetch_trading_fee(symbol))
                taker = fee.get('taker') if fee else None
                if taker is None or not math.isfinite(taker) or taker < 0:
                    raise ValueError(f'{venue} {symbol}: account taker fee unavailable')
                results[venue][symbol] = taker
                if taker > self.cfg['venues'][venue]['taker_fee'] + 1e-12:
                    raise ValueError(f'{venue} {symbol}: actual taker fee {taker} exceeds configured estimate; update fees and collect new evidence')
        return results

    async def refresh_balances(self):
        async def fetch(venue, ex):
            balance = await ex.fetch_balance()
            if not isinstance(balance.get('free'), dict) or not isinstance(balance.get('total'), dict):
                raise ValueError(f'{venue}: incomplete balance response')
            assets = {'USDT'} | {a for a,q in balance['total'].items() if q} | {s.split('/')[0] for m in self.cfg['arb_groups'].values() for v, s in m.items() if v == venue}
            # Total holdings establish drift; available holdings determine executability.
            return venue, {a: float(balance['free'].get(a) or 0) for a in assets}, {a: float(balance['total'].get(a) or 0) for a in assets}
        results = await asyncio.gather(*(fetch(v, e) for v, e in self.exchanges.items()))
        self.free_balances = {v: free for v, free, total in results}
        self.balances = {v: total for v, free, total in results}
        if any(not math.isfinite(q) or q < 0 for balances in (self.free_balances, self.balances) for assets in balances.values() for q in assets.values()):
            raise ValueError('Invalid account balances')

    async def execute_live(self, opp, store, prices):
        self.last_rejection = None
        if self.halted_reason:
            return None
        if self.private_enabled and any(s!='connected' for s in self.private_status.values()):
            self.last_rejection='Authenticated order streams are not ready'
            return None
        if opp.kind != 'cross' or opp.size > self.cfg['live']['max_trade_usdt']:
            self.last_rejection = 'live pilot only supports cross-venue routes within its size cap'
            return None
        # Private preflight happens first; books must still be fresh after the network roundtrip.
        try:
            await self.refresh_balances()
        except Exception:
            self.halt('Balance refresh failed; account state is uncertain')
            return None
        for item in opp.legs:
            if not store.usable(item['venue'], item['symbol'], self.cfg['live']['max_quote_age_ms']):
                self.last_rejection = 'quote expired during account preflight'
                return None
        if (time.time() - opp.ts) * 1000 > self.cfg['live']['max_quote_age_ms']:
            self.last_rejection = 'opportunity expired during account preflight'
            return None
        if self.inventory_skew_usdt(prices) > self.cfg['risk']['max_open_skew_usdt']:
            self.last_rejection = 'account inventory drift exceeds limit'
            return None
        if self.project(opp) is None or self.inventory_skew_usdt(prices, self.project(opp)) > self.cfg['risk']['max_open_skew_usdt']:
            self.last_rejection = 'projected inventory exceeds funded allocation or drift limit'
            return None
        portfolio_reasons=self.portfolio.check(self.project(opp),prices,self.baseline)
        if portfolio_reasons:
            self.last_rejection='; '.join(portfolio_reasons)
            return None
        orders = []
        for item in opp.legs:
            ex = self.exchanges[item['venue']]
            price = float(ex.price_to_precision(item['symbol'], item['limit_price']))
            # Precision may never loosen the quote's price bound.
            if (item['side'] == 'buy' and price > item['limit_price']) or (item['side'] == 'sell' and price < item['limit_price']):
                self.last_rejection = 'limit price requires a less conservative tick; skip'
                return None
            base, quote = item['symbol'].split('/')
            asset = quote if item['side'] == 'buy' else base
            need = price * item['qty'] * (1 + item['fee']) if item['side'] == 'buy' else item['qty']
            if self.free_balances[item['venue']].get(asset, 0) < need:
                self.last_rejection = f"insufficient free {asset} on {item['venue']}"
                return None
            orders.append((item, price, str(uuid.uuid4())))
        buy, sell = orders
        max_cost = buy[1] * buy[0]['qty'] * (1 + buy[0]['fee'])
        min_proceeds = sell[1] * sell[0]['qty'] * (1 - sell[0]['fee'])
        profit = min_proceeds - max_cost - opp.reserve
        if (max_cost > self.cfg['live']['max_trade_usdt'] or profit < self.cfg['scanner']['min_profit_usdt']
                or profit / max_cost < self.cfg['scanner']['min_net_edge']):
            self.last_rejection = 'IOC worst-case limits do not clear costs within pilot cap'
            return None
        self.mark_equity(prices)
        if self.risk_pnl_today() <= -self.cfg['risk']['max_daily_loss_usdt']:
            self.halt('daily live loss cap reached')
            return None
        from pathlib import Path
        # A private stream or disconnect guard can fail during balance refresh.
        # Recheck after every preflight await, immediately before durable intents.
        if self.halted_reason or (self.private_enabled and any(s!='connected' for s in self.private_status.values())):
            self.last_rejection=self.halted_reason or 'Authenticated order streams lost readiness'
            return None
        if Path(self.cfg['risk']['kill_switch_file']).exists():
            self.last_rejection = 'kill switch file present'
            return None
        # Write BOTH intents durably before sending either request. Never retry submission.
        batch_id=str(uuid.uuid4())
        with self.db:
            self.db.execute('INSERT INTO execution_batches VALUES(?,?,?,?,?)',
                            (batch_id,time.time(),json.dumps(asdict(opp)),json.dumps([o[2] for o in orders]),'intent'))
            for item, price, client in orders:
                self.db.execute('INSERT INTO live_orders VALUES(?,?,?,?,?,?,?,NULL,\'intent\',NULL)',
                    (client, time.time(), item['venue'], item['symbol'], item['side'], item['qty'], price))

        async def submit(item, price, client):
            ex = self.exchanges[item['venue']]
            changed=self._order_events.setdefault(client,asyncio.Event())
            params = {'timeInForce': 'IOC', 'clientOrderId': client}
            if item['venue'] == 'kraken':
                params['oflags'] = 'fciq'  # prefer quote-currency fees on both legs
            sent=time.monotonic()
            result = await ex.create_order(item['symbol'], 'limit', item['side'], item['qty'], price, params)
            if self.telemetry:
                self.telemetry.observe(f"{item['venue']}_order_ack_ms",(time.monotonic()-sent)*1000)
            if not result.get('id'):
                raise ValueError('Order acknowledgement missing exchange ID')
            with self.db:
                self.db.execute("UPDATE live_orders SET order_id=?,status='acknowledged',result=? WHERE client_id=?",
                    (result['id'], json.dumps(result), client))
            final = None
            for _ in range(5):
                if self.private_enabled:
                    try:
                        await asyncio.wait_for(changed.wait(),timeout=.3)
                    except asyncio.TimeoutError:
                        pass  # a REST check resolves missing or delayed stream updates
                    changed.clear()
                final = await ex.fetch_order(result['id'], item['symbol'])
                self.ledger.observe(client,final,'rest_reconciliation')
                if final.get('status') in ('closed', 'canceled', 'expired', 'rejected'):
                    break
                await asyncio.sleep(0.2)
            if final.get('status') not in ('closed', 'canceled', 'expired', 'rejected'):
                await ex.cancel_order(result['id'], item['symbol'])
                raise ValueError('IOC order did not settle promptly; cancellation requested')
            with self.db:
                self.db.execute("UPDATE live_orders SET status='terminal',result=? WHERE client_id=?", (json.dumps(final), client))
            return final

        results = await asyncio.gather(*(submit(*order) for order in orders), return_exceptions=True)
        if any(isinstance(r, BaseException) for r in results):
            self.halt('Order submission or reconciliation failed. Inspect live_orders and both exchange accounts; do not retry.')
            return None
        try:
            amounts = [float(r['filled']) for r in results]
            if (any(not math.isfinite(q) or q < 0 or q > opp.legs[0]['qty'] + 1e-12 for q in amounts)
                    or abs(amounts[0] - amounts[1]) > max(1e-12, opp.legs[0]['qty'] * 1e-9)):
                raise ValueError('Unmatched partial fills leave base exposure')
            if amounts[0] == 0:
                with self.db:
                    for _, _, client in orders:
                        self.db.execute("UPDATE live_orders SET status='settled' WHERE client_id=?", (client,))
                    self.db.execute("UPDATE execution_batches SET status='settled' WHERE id=?",(batch_id,))
                self.last_rejection = 'both IOC orders expired without fills'
                return None
            cash = []
            for item, result in zip(opp.legs, results):
                fees = result.get('fees') or ([result['fee']] if result.get('fee') else [])
                if not fees or any(f.get('currency') != 'USDT' or f.get('cost') is None for f in fees):
                    raise ValueError('Actual fee amount or quote fee currency is unavailable')
                fee = sum(float(f['cost']) for f in fees)
                cost = float(result['cost'])
                if not math.isfinite(cost) or not math.isfinite(fee) or cost <= 0 or fee < 0:
                    raise ValueError('Invalid fill accounting')
                cash.append(cost + fee if item['side'] == 'buy' else cost - fee)
            pnl = cash[1] - cash[0] - opp.reserve
            await self.refresh_balances()
            fill = Fill(opp.kind, opp.group, opp.detail, cash[0], pnl)
            with self.db:
                self.realized_pnl += pnl
                self.db.execute('INSERT INTO fills VALUES(?,?,?,?,?,?)', (fill.ts, fill.kind, fill.group, fill.detail, fill.notional, fill.pnl))
                for _, _, client in orders:
                    self.db.execute("UPDATE live_orders SET status='settled' WHERE client_id=?", (client,))
                self.db.execute("UPDATE execution_batches SET status='settled' WHERE id=?",(batch_id,))
                self._persist()
            self.fills.append(fill)
            self.fills = self.fills[-1000:]
            return fill
        except Exception as exc:
            self.halt(str(exc))
            return None

    async def aclose(self):
        for task in self._private_tasks:
            task.cancel()
        await asyncio.gather(*self._private_tasks,return_exceptions=True)
        await asyncio.gather(*(ex.close() for ex in self.exchanges.values()), return_exceptions=True)
        self.close()
