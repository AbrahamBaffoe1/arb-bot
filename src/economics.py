"""Account-backed fees, capital requirements and public break-even research."""
from __future__ import annotations
import asyncio
import hashlib
import json
import math
import os
import time
import ccxt.async_support as ccxt
from .models import Book, quote_for_base,normalized_depth


def finite_rate(value):
    return isinstance(value,(int,float)) and math.isfinite(value) and 0 <= value < 1


def break_even(buy_fee, sell_fee, slippage, rebalance):
    # sell_gross*(1-slip)*(1-fee_sell) must cover buy_gross*(1+slip)*(1+fee_buy)*(1+rebalance).
    return (1+slippage)*(1+buy_fee)*(1+rebalance)/((1-slippage)*(1-sell_fee))-1


def economics_report(cfg, accounts=None):
    accounts = accounts or {}
    rows = []
    budget = cfg['risk']['trade_notional_usdt']
    operating = cfg.get('economics',{}).get('operating_cost_usdt_per_day',0)
    for group, mapping in cfg['arb_groups'].items():
        for buy in mapping:
            for sell in mapping:
                if buy==sell or mapping[buy]!=mapping[sell]:
                    continue
                symbol=mapping[buy]
                bf=accounts.get(buy,{}).get('fees',{}).get(symbol,{})
                sf=accounts.get(sell,{}).get('fees',{}).get(symbol,{})
                verified=all(finite_rate(f.get(k)) for f in (bf,sf) for k in ('maker','taker'))
                for strategy in ('taker','maker'):
                    buy_fee=bf.get(strategy,cfg['venues'][buy][strategy+'_fee'])
                    sell_fee=sf.get('taker',cfg['venues'][sell]['taker_fee'])
                    slip=cfg['scanner']['slippage_buffer']
                    edge=break_even(buy_fee,sell_fee,slip,cfg['scanner']['rebalance_buffer'])
                    row=dict(group=group,symbol=symbol,buy_venue=buy,sell_venue=sell,
                        strategy=strategy,fees_verified=verified,entry_fee=buy_fee,hedge_fee=sell_fee,
                        break_even_gross_edge=edge,buy_cash_budget_usdt=budget,
                        sell_inventory_value_usdt=budget,minimum_committed_capital_usdt=budget*2,
                        # Operating costs cannot honestly be assigned per trade without observed turnover.
                        daily_operating_cost_usdt=operating)
                    row['operating_cost_sensitivity']=[dict(completed_hedges_per_day=n,
                        break_even_gross_edge=(1+edge)*(1+operating/(n*budget*(1+cfg['scanner']['rebalance_buffer'])))-1)
                        for n in (1,10,100)]
                    bb=accounts.get(buy,{}).get('balances',{})
                    sb=accounts.get(sell,{}).get('balances',{})
                    row.update(available_buy_cash_usdt=bb.get('USDT',{}).get('free'),
                               available_hedge_base_qty=sb.get(symbol.split('/')[0],{}).get('free'),
                               inventory_capacity_verified=False)
                    book=accounts.get(buy,{}).get('books',{}).get(symbol)
                    hedge=accounts.get(sell,{}).get('books',{}).get(symbol)
                    if book and hedge and bb is not None and sb is not None:
                        px=book['bids' if strategy=='maker' else 'asks'][0][0]
                        cash=bb.get('USDT',{}).get('free',0)
                        inventory=sb.get(symbol.split('/')[0],{}).get('free',0)
                        qty=min(budget,cash)/(px*(1+buy_fee)*(1+slip))
                        qty=min(qty,inventory)
                        raw=quote_for_base(hedge['bids'],qty) if qty>0 else 0
                        entry=qty*px if strategy=='maker' else quote_for_base(book['asks'],qty)
                        row.update(indicative_capacity_base_qty=qty,inventory_capacity_verified=verified,
                                   snapshot_depth_sufficient=raw is not None and entry is not None,
                                   market_snapshot_age_s=max(time.time()-book['received'],time.time()-hedge['received']))
                        if raw is not None and entry is not None:
                            cost=entry*(1+buy_fee)*(1+slip)
                            net=raw*(1-sell_fee)*(1-slip)-cost*(1+cfg['scanner']['rebalance_buffer'])
                            row.update(indicative_profit_before_operating_usdt=net,
                                       operating_break_even_hedges_per_day=math.ceil(operating/net) if net>0 else None)
                    rows.append(row)
    enabled={v for v,c in cfg['venues'].items() if c.get('enabled')}
    return dict(account_verified=enabled<=accounts.keys() and bool(rows) and all(r['fees_verified'] for r in rows),
                costs_verified=False,rows=rows,accounts=accounts,
                note='Capital includes cash and sell inventory; acquisition, custody and transfer costs depend on actual balances. Maker fill risk is not represented by the break-even calculation.')


class AccountInspector:
    def __init__(self,cfg):
        self.cfg=cfg
        self.exchanges={}

    async def inspect(self):
        missing=[v for v,c in self.cfg['venues'].items() if c.get('enabled') and
                 not all(os.getenv(v.upper()+suffix) for suffix in ('_API_KEY','_SECRET'))]
        if missing:
            raise ValueError('Account verification blocked: configure local credentials for '+', '.join(missing))
        async def venue_report(venue):
            key=os.environ[venue.upper()+'_API_KEY']
            ex=getattr(ccxt,venue)({'apiKey':key,'secret':os.environ[venue.upper()+'_SECRET'].replace('\\n','\n'),
                                  'enableRateLimit':True,'timeout':15000})
            self.exchanges[venue]=ex
            await ex.load_markets()
            symbols=sorted({m[venue] for m in self.cfg['arb_groups'].values() if venue in m})
            all_fees=await ex.fetch_trading_fees({'type':'spot'}) if ex.has.get('fetchTradingFees') is True else None
            fees={}
            for symbol in symbols:
                if symbol not in ex.markets:
                    raise ValueError(f'{venue}: configured market unavailable: {symbol}')
                item=all_fees.get(symbol) if all_fees is not None else await ex.fetch_trading_fee(symbol)
                if not item or not all(finite_rate(item.get(k)) for k in ('maker','taker')):
                    raise ValueError(f'{venue} {symbol}: maker/taker fees unavailable')
                fees[symbol]={k:item[k] for k in ('maker','taker')}
            balance=await ex.fetch_balance()
            if not all(isinstance(balance.get(k),dict) for k in ('free','total')):
                raise ValueError(f'{venue}: incomplete account balances')
            balances={a:dict(total=float(q),free=float(balance['free'].get(a) or 0)) for a,q in balance['total'].items() if q}
            if any(not math.isfinite(x) or x<0 for b in balances.values() for x in b.values()):
                raise ValueError(f'{venue}: invalid account balances')
            books={}
            for symbol in symbols:
                book=normalized_depth(await ex.fetch_order_book(symbol,self.cfg['scanner']['book_depth']))
                if not Book(venue,symbol,book['bids'],book['asks']).valid():
                    raise ValueError(f'{venue} {symbol}: invalid economics depth snapshot')
                books[symbol]=dict(bids=book['bids'],asks=book['asks'],received=time.time(),timestamp=book.get('timestamp'))
            return venue,dict(verified_at=time.time(),account_identity=hashlib.sha256((venue+key).encode()).hexdigest(),
                              fees=fees,balances=balances,books=books,capabilities={k:bool(ex.has.get(k)) for k in ('fetchOpenOrders','fetchClosedOrders','cancelAllOrdersAfter')})
        results=await asyncio.gather(*(venue_report(v) for v,c in self.cfg['venues'].items() if c.get('enabled')))
        return dict(results)

    async def close(self):
        await asyncio.gather(*(ex.close() for ex in self.exchanges.values()),return_exceptions=True)


def save_receipts(db,cfg,accounts):
    """Only fees at or below modeled costs can certify existing observations."""
    for venue,account in accounts.items():
        for symbol,fee in account['fees'].items():
            for kind in ('maker','taker'):
                if fee[kind]>cfg['venues'][venue][kind+'_fee']+1e-12:
                    raise ValueError(f'{venue} {symbol} actual {kind} fee {fee[kind]} exceeds configuration. Update costs and start new evidence.')
    db.execute('CREATE TABLE IF NOT EXISTS account_receipts(venue TEXT PRIMARY KEY,verified REAL,body TEXT)')
    db.execute('CREATE TABLE IF NOT EXISTS fee_verifications(venue TEXT PRIMARY KEY,ts REAL,rates TEXT)')
    with db:
        for venue,account in accounts.items():
            db.execute('INSERT OR REPLACE INTO account_receipts VALUES(?,?,?)',(venue,account['verified_at'],json.dumps(account)))
            db.execute('INSERT OR REPLACE INTO fee_verifications VALUES(?,?,?)',
                       (venue,account['verified_at'],json.dumps({s:f['taker'] for s,f in account['fees'].items()})))
