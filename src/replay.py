"""Chronological maker-entry/IOC-hedge simulator. Research only; never submits orders."""
from __future__ import annotations
import copy
import heapq
import itertools
import json
import math
import sqlite3
from pathlib import Path
import ccxt
from .models import Book, quote_for_base
from .recorder import read_events


class MakerReplay:
    def __init__(self,cfg,entry_delay_ms=200,hedge_delay_ms=300,quote_lifetime_ms=2000,
                 cancel_delay_ms=200,queue_multiplier=1.0,depth_fraction=1.0,min_edge=None):
        self.cfg=copy.deepcopy(cfg)
        self.entry_delay=entry_delay_ms/1000
        self.hedge_delay=hedge_delay_ms/1000
        self.lifetime=quote_lifetime_ms/1000
        self.cancel_delay=cancel_delay_ms/1000
        self.queue_multiplier=queue_multiplier
        self.depth_fraction=depth_fraction
        self.min_edge=cfg['scanner']['min_net_edge'] if min_edge is None else min_edge
        if not all(math.isfinite(x) for x in (entry_delay_ms,hedge_delay_ms,quote_lifetime_ms,cancel_delay_ms,queue_multiplier,depth_fraction,self.min_edge)) or min(entry_delay_ms,hedge_delay_ms,quote_lifetime_ms,cancel_delay_ms,queue_multiplier)<0 or not 0<depth_fraction<=1:
            raise ValueError('Replay delays/queue must be nonnegative and depth_fraction in (0,1]')
        start=cfg['paper']['starting_balance_usdt']
        self.balances={v:{'USDT':float(start)} for v,c in cfg['venues'].items() if c.get('enabled')}
        self.initial_capital=sum(b['USDT'] for b in self.balances.values())
        self.books={}
        self.adapters={}
        self.seeded=set()
        self.marks={}
        self.orders={}
        self.pending=[]
        self.seq=itertools.count()
        self.log=[]
        self.filled_qty=0.0
        self.hedged_qty=0.0
        self.fills=0
        self.hedges=0
        self.cancelled=0
        self.rejected=0
        self.unmatched={}
        self.stopped=False
        self.peak=self.initial_capital
        self.max_drawdown=0.0
        self.first=None
        self.last=None
        self.now=0
        self.consumed={}
        self.last_quote={}
        self.missing_source_trades=0
        self.seen_trades=set()
        self.integrity_ok=True

    def halt(self,reason):
        if not self.stopped:
            self.log.append(dict(ts=self.now,type='halt',reason=reason))
        self.stopped=True
        # Resting orders can still fill until cancellation reaches the venue.
        for order in self.orders.values():
            if not order.get('cancel_requested'):
                order['cancel_requested']=True
                self.schedule(self.now+self.cancel_delay,'cancel',order)

    def fresh(self,venue,symbol):
        b=self.books.get((venue,symbol))
        if not b or not b['bids'] or not b['asks']:
            return False
        age=self.cfg['scanner']['stale_book_ms']/1000
        return 0<=self.now-b['ts']<=age and (b['source'] is None or -.1<=self.now-b['source']<=age)

    def amount(self,v,s,q):
        try:
            return float(self.adapters[v].amount_to_precision(s,q))
        except (ccxt.BaseError,KeyError):
            return 0.0

    def limits(self,v,s,q,cost):
        market=self.adapters[v].markets.get(s,{})
        for key,val in (('amount',q),('cost',cost)):
            limits=market.get('limits',{}).get(key) or {}
            if limits.get('min') is not None and val<limits['min']: return False
            if limits.get('max') is not None and val>limits['max']: return False
        return q>0

    def equity(self):
        return sum(q if a=='USDT' else q*self.marks.get(a,0) for assets in self.balances.values() for a,q in assets.items())

    def seed(self,v,s):
        base,quote=s.split('/')
        if quote!='USDT' or (v,base) in self.seeded: return
        count=sum(v in m for m in self.cfg['arb_groups'].values())
        budget=self.cfg['paper']['starting_balance_usdt']*self.cfg['paper']['inventory_fraction']/max(1,count)
        fee=self.cfg['venues'][v]['taker_fee']
        slip=self.cfg['scanner']['slippage_buffer']
        b=self.books[(v,s)]
        px=b['asks'][0][0]
        qty=self.amount(v,s,budget/(px*(1+fee)*(1+slip)))
        raw=quote_for_base(b['asks'],qty)
        if raw is None or not self.limits(v,s,qty,raw): return
        cost=raw*(1+fee)*(1+slip)
        if cost>budget+1e-9 or cost>self.balances[v]['USDT']: return
        self.balances[v]['USDT']-=cost
        self.balances[v][base]=self.balances[v].get(base,0)+qty
        self.seeded.add((v,base))

    def schedule(self,when,kind,payload):
        heapq.heappush(self.pending,(when,next(self.seq),kind,payload))

    def process_due(self,until):
        # Timers strictly before a received event cannot use that future event's book.
        while self.pending and self.pending[0][0]<=until:
            self.now,_,kind,payload=heapq.heappop(self.pending)
            if kind=='activate': self.activate(payload)
            elif kind=='cancel':
                if self.orders.get(payload['key']) is payload:
                    self.cancelled+=1
                    self.orders.pop(payload['key'])
            elif kind=='hedge': self.hedge(payload)

    def activate(self,order):
        key=order['key']
        if self.orders.get(key) is not order:
            return
        if self.stopped or not self.fresh(order['venue'],order['symbol']):
            self.orders.pop(key,None); self.rejected+=1; return
        b=self.books[(order['venue'],order['symbol'])]
        if order['price']>=b['asks'][0][0]:
            self.orders.pop(key,None); self.rejected+=1; return
        order['active']=True
        order['activated']=self.now
        order['ahead']=sum(q for p,q in b['bids'] if p>=order['price'])*self.queue_multiplier
        self.schedule(self.now+self.lifetime+self.cancel_delay,'cancel',order)

    def quote(self):
        if self.stopped: return
        for base,mapping in self.cfg['arb_groups'].items():
            for buy,s in mapping.items():
                for sell,other in mapping.items():
                    key=(buy,s)
                    if buy==sell or s!=other or key in self.orders or s.split('/')[1]!='USDT': continue
                    if self.now-self.last_quote.get(key,-math.inf)<self.cfg['risk']['cooldown_s']: continue
                    if not self.fresh(buy,s) or not self.fresh(sell,s): continue
                    a,b=self.books[(buy,s)],self.books[(sell,s)]
                    if abs(a['ts']-b['ts'])*1000>self.cfg['scanner']['max_book_skew_ms']: continue
                    price=a['bids'][0][0]
                    fee=self.cfg['venues'][buy]['maker_fee']
                    budget=self.cfg['risk']['trade_notional_usdt']
                    qty=self.amount(buy,s,budget/(price*(1+fee)))
                    qty=min(qty,self.amount(sell,s,qty))
                    if qty<=0 or self.amount(buy,s,qty)!=qty: continue
                    raw=quote_for_base(b['bids'],qty)
                    if raw is None or not self.limits(buy,s,qty,price*qty) or not self.limits(sell,s,qty,raw): continue
                    cost=qty*price*(1+fee)
                    proceeds=raw*(1-self.cfg['venues'][sell]['taker_fee'])*(1-self.cfg['scanner']['slippage_buffer'])
                    net=proceeds-cost-cost*self.cfg['scanner']['rebalance_buffer']
                    if net<self.cfg['scanner']['min_profit_usdt'] or net/cost<self.min_edge: continue
                    reserved=sum(o['remaining'] for o in self.orders.values() if o['hedge']==sell and o['symbol']==s)
                    pending_qty=sum(p[3]['qty'] for p in self.pending if p[2]=='hedge' and p[3]['venue']==sell and p[3]['symbol']==s)
                    if self.balances[sell].get(base,0)<qty+reserved+pending_qty: continue
                    reserved_cash=sum(o['remaining']*o['price']*(1+self.cfg['venues'][buy]['maker_fee'])
                                      for o in self.orders.values() if o['venue']==buy)
                    if self.balances[buy]['USDT']-reserved_cash<cost: continue
                    order=dict(key=key,venue=buy,hedge=sell,symbol=s,price=price,qty=qty,remaining=qty,
                               active=False,created=self.now,ahead=0)
                    self.orders[key]=order
                    self.last_quote[key]=self.now
                    self.schedule(self.now+self.entry_delay,'activate',order)
                    self.log.append(dict(ts=self.now,type='quote',venue=buy,hedge=sell,symbol=s,qty=qty,price=price))

    def trade(self,e):
        order=self.orders.get((e['venue'],e['symbol']))
        if not order or not order['active']: return
        t=e['body']
        if t.get('id') is not None:
            key=(e['venue'],e['symbol'],str(t['id']))
            if key in self.seen_trades: return
            self.seen_trades.add(key)
        source=e['source_ts']
        if source is None:
            self.missing_source_trades+=1; return
        # Historical/reordered cached trades cannot fill an order that didn't exist yet.
        if source<order['activated'] or source>self.now+.1 or self.now-source>self.cfg['scanner']['stale_book_ms']/1000: return
        side=t.get('side')
        # Legacy Coinbase tapes saved CCXT's maker-side field verbatim.
        basis=t.get('side_basis','maker' if e['venue']=='coinbase' else 'taker')
        if basis=='maker':side={'buy':'sell','sell':'buy'}.get(side)
        if side!='sell' or not isinstance(t.get('price'),(int,float)) or not math.isfinite(t['price']) or t['price']<=0 or t['price']>order['price']: return
        volume=float(t.get('amount') or 0)
        if not math.isfinite(volume) or volume<=0: return
        consume=min(volume,order['ahead'])
        order['ahead']-=consume
        volume-=consume
        qty=min(order['remaining'],volume)
        if qty<=1e-12: return
        base=e['symbol'].split('/')[0]
        fee=self.cfg['venues'][e['venue']]['maker_fee']
        cost=qty*order['price']*(1+fee)
        self.balances[e['venue']]['USDT']-=cost
        self.balances[e['venue']][base]=self.balances[e['venue']].get(base,0)+qty
        order['remaining']-=qty
        self.filled_qty+=qty
        self.fills+=1
        self.unmatched[base]=self.unmatched.get(base,0)+qty
        self.schedule(self.now+self.hedge_delay,'hedge',dict(venue=order['hedge'],symbol=e['symbol'],qty=qty,cost=cost))
        self.log.append(dict(ts=self.now,type='maker_fill',venue=e['venue'],symbol=e['symbol'],qty=qty,cost=cost))
        if order['remaining']<=1e-12: self.orders.pop(order['key'],None)

    def hedge(self,p):
        v,s=p['venue'],p['symbol']
        base=s.split('/')[0]
        qty=self.amount(v,s,p['qty'])
        filled=0.0; raw=0.0
        if self.fresh(v,s):
            b=self.books[(v,s)]
            key=(v,s,b['revision'])
            used=self.consumed.get(key,0)
            remaining=min(qty,self.balances[v].get(base,0))
            for price,volume in b['bids']:
                available=volume*self.depth_fraction
                skip=min(used,available); used-=skip; available-=skip
                take=min(remaining,available)
                filled+=take; raw+=take*price; remaining-=take
                if remaining<=1e-12: break
            rounded=self.amount(v,s,filled)
            # Rounding removes the final (worst-priced) slice, not a pro-rata
            # slice from every level. Reprice the rounded quantity exactly.
            remaining=rounded; raw=0.0; used=self.consumed.get(key,0)
            for price,volume in b['bids']:
                available=volume*self.depth_fraction
                skip=min(used,available); used-=skip; available-=skip
                take=min(remaining,available); raw+=take*price; remaining-=take
                if remaining<=1e-12: break
            filled=rounded
            if not self.limits(v,s,filled,raw): filled=0;raw=0
            self.consumed[key]=self.consumed.get(key,0)+filled
        if filled:
            proceeds=raw*(1-self.cfg['venues'][v]['taker_fee'])*(1-self.cfg['scanner']['slippage_buffer'])
            reserve=p['cost']*self.cfg['scanner']['rebalance_buffer']
            self.balances[v][base]-=filled
            self.balances[v]['USDT']+=proceeds-reserve
            self.unmatched[base]-=filled
            self.hedged_qty+=filled
            self.hedges+=1
        residual=p['qty']-filled
        self.log.append(dict(ts=self.now,type='hedge',venue=v,symbol=s,requested=p['qty'],filled=filled,residual=residual))
        if residual*self.marks.get(base,0)>self.cfg.get('research',{}).get('max_unhedged_usdt',.01):
            self.halt('Unmatched hedge exposure')

    def consume(self,e,allow_quotes=True):
        ts=e['received']
        if self.last is not None and ts<self.last:
            self.integrity_ok=False
            self.halt('Nonmonotonic receive clock')
            return
        self.process_due(ts)
        self.now=ts
        self.first=ts if self.first is None else self.first
        self.last=ts
        v,s=e['venue'],e['symbol']
        if e['kind']=='markets':
            ex=getattr(ccxt,v)(); ex.markets=e['body']['markets']; ex.precisionMode=e['body']['precision_mode']; self.adapters[v]=ex
        elif e['kind']=='book':
            body=e['body']
            bids,asks=body['bids'],body['asks']
            if not Book(v,s,bids,asks).valid():
                self.books.pop((v,s),None)
                self.integrity_ok=False
                self.halt('Invalid recorded book')
                return
            self.books[(v,s)]=dict(bids=bids,asks=asks,revision=e['id'],ts=ts,source=e['source_ts'])
            # Drop depth consumption for prior revisions to bound replay memory.
            self.consumed={k:val for k,val in self.consumed.items() if k[:2]!=(v,s)}
            if s.split('/')[1]=='USDT':
                self.marks[s.split('/')[0]]=bids[0][0]  # liquidation mark, not midpoint
                if v in self.adapters and self.fresh(v,s): self.seed(v,s)
            if allow_quotes: self.quote()
        elif e['kind']=='trade': self.trade(e)
        elif e['kind']=='feed_error':
            if s: self.books.pop((v,s),None)
            else: self.books={k:b for k,b in self.books.items() if k[0]!=v}
            for key,order in list(self.orders.items()):
                if order['venue']==v or order['hedge']==v:
                    self.schedule(self.now+self.cancel_delay,'cancel',order)
        elif e['kind']=='recording_gap':
            self.integrity_ok=False
            self.halt('Recording gap')
        equity=self.equity()
        self.peak=max(self.peak,equity)
        self.max_drawdown=max(self.max_drawdown,(self.peak-equity)/self.peak if self.peak else 0)
        if self.max_drawdown>self.cfg.get('portfolio',{}).get('max_drawdown_fraction',.05):
            self.halt('Portfolio drawdown limit')

    def report(self):
        duration=max(0,(self.last or 0)-(self.first or 0))
        operating=self.cfg.get('economics',{}).get('operating_cost_usdt_per_day',0)*duration/86400
        equity=self.equity()-operating
        unhedged=sum(max(0,q)*self.marks.get(a,0) for a,q in self.unmatched.items())
        return dict(maker_fill_events=self.fills,hedge_events=self.hedges,quotes=sum(x['type']=='quote' for x in self.log),
                    cancellations=self.cancelled,rejected_entries=self.rejected,
                    initial_capital_usdt=self.initial_capital,final_marked_equity_usdt=equity,
                    net_pnl_usdt=equity-self.initial_capital,return_on_committed_capital=(equity/self.initial_capital-1),
                    max_drawdown_fraction=self.max_drawdown,unhedged_usdt=unhedged,
                    pending_hedges=sum(p[2]=='hedge' for p in self.pending),open_quotes=len(self.orders),
                    data_integrity_ok=self.integrity_ok,
                    stopped=self.stopped,duration_s=duration,operating_cost_usdt=operating,
                    missing_source_trades=self.missing_source_trades,
                    limitations='Simulated queue and fills, not proof of execution. Unhedged inventory is marked at last bids. No future book is used for delayed hedge execution.')


def session_config(path,session):
    with sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro',uri=True) as db:
        row=db.execute('SELECT config,ended FROM sessions WHERE id=?',(session,)).fetchone()
        if not row: raise ValueError('Unknown recording session')
        return json.loads(row[0]),row[1]


def replay(path,session,params=None,start=None,end=None):
    cfg,ended=session_config(path,session)
    sim=MakerReplay(cfg,**(params or {}))
    # Pre-period metadata is allowed; trading books and fills are not warmed from future events.
    for e in read_events(path,session):
        if start is not None and e['received']<start:
            if e['kind']=='markets':
                sim.consume(e,allow_quotes=False)
                sim.first=sim.last=None
            continue
        if end is not None and e['received']>=end: break
        sim.consume(e)
    result=sim.report()
    result.update(session=session,parameters=params or {},recording_closed=ended is not None)
    return result,sim.log


def walk_forward(path,session,train_fraction=.6):
    if not 0<train_fraction<1: raise ValueError('train_fraction must be between zero and one')
    cfg,ended=session_config(path,session)
    if ended is None: raise ValueError('Close the recording session before walk-forward evaluation')
    with sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro',uri=True) as db:
        lo,hi=db.execute('SELECT min(received),max(received) FROM events WHERE session=? AND kind IN (\'book\',\'trade\')',(session,)).fetchone()
    if lo is None or hi<=lo: raise ValueError('Recording has insufficient market events')
    split=lo+(hi-lo)*train_fraction
    candidates=[{'quote_lifetime_ms':ttl,'min_edge':edge} for ttl in (1000,3000)
                for edge in (cfg['scanner']['min_net_edge'],cfg['scanner']['min_net_edge']*2)]
    training=[replay(path,session,p,end=split)[0] for p in candidates]
    def settled(r):
        return r['data_integrity_ok'] and not r['stopped'] and r['unhedged_usdt']<.01 and not r['pending_hedges'] and not r['open_quotes']
    eligible=[r for r in training if r['maker_fill_events']>0 and settled(r)]
    selected=max(eligible,key=lambda r:r['net_pnl_usdt'])['parameters'] if eligible else candidates[0]
    test,log=replay(path,session,selected,start=split)
    stress,_=replay(path,session,dict(selected,hedge_delay_ms=1000,queue_multiplier=2,depth_fraction=.5),start=split)
    return dict(session=session,split_ts=split,training=training,selected_parameters=selected,
                training_selection_supported=bool(eligible),out_of_sample=test,stress=stress,
                verdict='Research candidate only' if eligible and test['maker_fill_events']>0 and test['net_pnl_usdt']>0 and stress['net_pnl_usdt']>0 and settled(test) and settled(stress) else 'No validated edge; do not fund this strategy',
                live_ready=False),log
