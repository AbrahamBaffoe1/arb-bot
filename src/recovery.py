"""Order observations and read-only recovery audits. Unknown intent is never safe to retry."""
from __future__ import annotations
import json
import math
import time
import asyncio
from pathlib import Path

TERMINAL={'closed','canceled','expired','rejected'}


class DisconnectGuard:
    """Renew Kraken's account-wide cancellation timer only while trading is healthy."""
    def __init__(self,exchange,halt,kill_file,healthy,timeout_ms=60000,renew_s=20):
        if timeout_ms<=0 or not 0<renew_s<timeout_ms/1000:
            raise ValueError('Disconnect timer must exceed its renewal interval')
        self.exchange=exchange
        self.halt=halt
        self.kill_file=Path(kill_file)
        self.healthy=healthy
        self.timeout_ms=timeout_ms
        self.renew_s=renew_s
        self.armed=False

    async def arm(self):
        if self.kill_file.exists():
            raise RuntimeError('Kill switch present; disconnect guard cannot start')
        if self.exchange.has.get('cancelAllOrdersAfter') is not True:
            raise RuntimeError('Kraken disconnect cancellation is unavailable')
        await self.exchange.cancel_all_orders_after(self.timeout_ms)
        self.armed=True

    async def run(self):
        try:
            while True:
                await asyncio.sleep(self.renew_s)
                if self.kill_file.exists() or not self.healthy():
                    self.halt('Disconnect guard stopped renewal; reconcile orders after timer expiry')
                    return
                await self.exchange.cancel_all_orders_after(self.timeout_ms)
        except asyncio.CancelledError:
            # Leave the existing timer armed on shutdown; never clear protection
            # while an unknown order might still be resting.
            raise
        except Exception:
            self.halt('Disconnect guard renewal failed; order reconciliation required')


class OrderLedger:
    def __init__(self,db):
        self.db=db
        db.executescript('''
            CREATE TABLE IF NOT EXISTS order_events(id INTEGER PRIMARY KEY,client_id TEXT,ts REAL,source TEXT,status TEXT,body TEXT);
            CREATE TABLE IF NOT EXISTS order_observations(client_id TEXT PRIMARY KEY,filled REAL,status TEXT,body TEXT,ts REAL);
            CREATE TABLE IF NOT EXISTS execution_batches(id TEXT PRIMARY KEY,ts REAL,plan TEXT,clients TEXT,status TEXT);
            CREATE TABLE IF NOT EXISTS recovery_audits(id INTEGER PRIMARY KEY,ts REAL,body TEXT);
        ''')
        db.commit()

    def observe(self,client_id,order,source):
        filled=order.get('filled')
        # Some websocket updates omit cumulative fills. Record the observation but
        # never use an incomplete update as a settlement acknowledgement.
        old=self.db.execute('SELECT filled,status,body FROM order_observations WHERE client_id=?',(client_id,)).fetchone()
        if filled is not None and (not isinstance(filled,(int,float)) or not math.isfinite(filled) or filled<0):
            raise ValueError('Invalid cumulative fill observation')
        status=order.get('status') or 'unknown'
        if old:
            if filled is not None and filled+1e-12<old[0]:
                raise ValueError('Cumulative order fill decreased')
            previous=json.loads(old[2])
            if previous.get('id') and order.get('id') and previous['id']!=order['id']:
                raise ValueError('Exchange order ID changed for a client intent')
            if old[1] in TERMINAL and status not in TERMINAL:
                # Late open snapshots are retained for audit, not allowed to reopen a terminal order.
                status=old[1]
        with self.db:
            self.db.execute('INSERT INTO order_events(client_id,ts,source,status,body) VALUES(?,?,?,?,?)',
                            (client_id,time.time(),source,order.get('status'),json.dumps(order)))
            if filled is not None:
                merged=json.loads(old[2]) if old else {}
                merged.update({k:v for k,v in order.items() if v is not None})
                merged['status']=status
                self.db.execute('INSERT OR REPLACE INTO order_observations VALUES(?,?,?,?,?)',
                                (client_id,filled,status,json.dumps(merged),time.time()))

    async def audit(self,exchanges):
        rows=self.db.execute("SELECT client_id,ts,venue,symbol,side,qty,order_id,status FROM live_orders WHERE status!='settled' ORDER BY ts").fetchall()
        reports=[]
        for client,ts,venue,symbol,side,qty,order_id,status in rows:
            ex=exchanges[venue]
            try:
                if order_id:
                    order=await ex.fetch_order(order_id,symbol)
                else:
                    # Bounded search. Absence is explicitly inconclusive (pagination/history limits).
                    opened=await ex.fetch_open_orders(symbol)
                    closed=await ex.fetch_closed_orders(symbol,int((ts-60)*1000),1000)
                    matches=[o for o in opened+closed if o.get('clientOrderId')==client]
                    ids={o.get('id') for o in matches}
                    if len(ids)!=1:
                        reports.append(dict(client_id=client,venue=venue,symbol=symbol,status='unknown',
                                            reason='Client ID not uniquely resolved in bounded exchange history; do not resubmit'))
                        continue
                    order=await ex.fetch_order(next(iter(ids)),symbol)
                if order.get('symbol') not in (None,symbol) or order.get('side') not in (None,side):
                    raise ValueError('Exchange order does not match recorded intent')
                if (order_id and order.get('id')!=order_id) or order.get('clientOrderId') not in (None,client):
                    raise ValueError('Exchange order identity does not match recorded intent')
                if order.get('filled') is not None and order['filled']>qty+1e-12:
                    raise ValueError('Order observation exceeds intended quantity')
                self.observe(client,order,'recovery_rest')
                reports.append(dict(client_id=client,venue=venue,symbol=symbol,order_id=order.get('id'),
                    status=order.get('status'),filled=order.get('filled'),cost=order.get('cost'),
                    fees=order.get('fees') or order.get('fee'),requested_qty=qty,
                    requires_cancellation=order.get('status') not in TERMINAL))
            except Exception as exc:
                reports.append(dict(client_id=client,venue=venue,symbol=symbol,status='unknown',reason=type(exc).__name__))
        batches=[]
        for batch,plan,clients,status in self.db.execute("SELECT id,plan,clients,status FROM execution_batches WHERE status!='settled'"):
            ids=json.loads(clients)
            by_client={r['client_id']:r for r in reports}
            found=[by_client[c] for c in ids if c in by_client]
            settled=len(found)==len(ids) and all(r['status'] in TERMINAL and r.get('filled') is not None for r in found)
            qty_delta=(found[0]['filled']-found[1]['filled']) if settled and len(found)==2 else None
            batches.append(dict(batch_id=batch,orders_terminal=settled,unmatched_base_qty=qty_delta,
                                action='Operator must reconcile fees and balances before clearing the halt'))
        result=dict(ts=time.time(),orders=reports,batches=batches,orders_submitted=0,
                    halt_cleared=False,unresolved=sum(r['status']=='unknown' for r in reports))
        with self.db:
            self.db.execute('INSERT INTO recovery_audits(ts,body) VALUES(?,?)',(result['ts'],json.dumps(result)))
        return result
