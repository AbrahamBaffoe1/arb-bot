"""Rank shared spot markets using real tickers; discoveries never modify live routing."""
import asyncio
import math
import ccxt.async_support as ccxt
from .models import Book,quote_for_base,normalized_depth


def depth_route(cfg,exchanges,symbol,buy,sell,books):
    books={v:normalized_depth(b) for v,b in books.items()}
    a,b=books[buy],books[sell]
    if not all(Book(v,symbol,book['bids'],book['asks']).valid() for v,book in books.items()):
        return None
    fee=cfg['venues'][buy]['maker_fee']
    budget=cfg['risk']['trade_notional_usdt']
    price=a['bids'][0][0]
    qty=float(exchanges[buy].amount_to_precision(symbol,budget/(price*(1+fee))))
    qty=min(qty,float(exchanges[sell].amount_to_precision(symbol,qty)))
    if qty<=0 or float(exchanges[buy].amount_to_precision(symbol,qty))!=qty: return None
    proceeds=quote_for_base(b['bids'],qty)
    if proceeds is None: return None
    cost=price*qty*(1+fee)
    for v,amount in ((buy,price*qty),(sell,proceeds)):
        limits=exchanges[v].markets[symbol].get('limits') or {}
        for key,value in (('amount',qty),('cost',amount)):
            bound=limits.get(key) or {}
            if bound.get('min') is not None and value<bound['min']: return None
            if bound.get('max') is not None and value>bound['max']: return None
    net=proceeds*(1-cfg['venues'][sell]['taker_fee'])*(1-cfg['scanner']['slippage_buffer'])-cost*(1+cfg['scanner']['rebalance_buffer'])
    return dict(buy_venue=buy,hedge_venue=sell,qty=qty,entry_price=price,
                indicative_net_profit_quote=net,indicative_net_edge=net/cost,
                minimum_committed_capital_quote=cost+proceeds)


async def discover(cfg):
    exchanges={v:getattr(ccxt,v)({'enableRateLimit':True,'timeout':20000}) for v in ('coinbase','kraken')}
    try:
        await asyncio.gather(*(ex.load_markets() for ex in exchanges.values()))
        shared=set.intersection(*[{s for s,m in ex.markets.items() if m.get('spot') and m.get('active') is not False
                                  and m.get('quote') in cfg.get('research',{}).get('discovery_quotes',['USDT','USD'])}
                                for ex in exchanges.values()])
        # Deterministic cap bounds API work; candidates remain research-only.
        symbols=sorted(shared)
        tickers=await asyncio.gather(*(ex.fetch_tickers(symbols) for ex in exchanges.values()))
        reports=[]
        for symbol in symbols:
            items=[t.get(symbol) for t in tickers]
            if any(not t for t in items):
                continue
            volumes=[t.get('quoteVolume') or (t.get('baseVolume') or 0)*(t.get('last') or 0) for t in items]
            if any(not math.isfinite(v) or v<=0 for v in volumes):
                continue
            quoted=all(t.get('bid') and t.get('ask') for t in items)
            spread=max(items[1]['bid']/items[0]['ask']-1,items[0]['bid']/items[1]['ask']-1) if quoted else None
            reports.append(dict(symbol=symbol,quote=symbol.split('/')[1],min_venue_quote_volume_24h=min(volumes),
                                gross_cross_edge=spread,coinbase_bid=items[0].get('bid'),coinbase_ask=items[0].get('ask'),
                                kraken_bid=items[1].get('bid'),kraken_ask=items[1].get('ask'),
                                executable=False,reason='Ticker screening only; requires depth, fee, inventory and replay validation'))
        reports=sorted(reports,key=lambda r:r['min_venue_quote_volume_24h'],reverse=True)[:cfg.get('research',{}).get('max_discovery_markets',100)]
        for row in reports[:cfg.get('research',{}).get('discovery_depth_markets',10)]:
            try:
                books=dict(zip(exchanges,await asyncio.gather(*(ex.fetch_order_book(row['symbol'],cfg['scanner']['book_depth']) for ex in exchanges.values()))))
                routes=[depth_route(cfg,exchanges,row['symbol'],buy,sell,books)
                        for buy,sell in (('coinbase','kraken'),('kraken','coinbase'))]
                routes=[r for r in routes if r]
                row['maker_depth_routes']=sorted(routes,key=lambda r:r['indicative_net_edge'],reverse=True)
                for venue,book in books.items():
                    row[venue+'_bid']=book['bids'][0][0]
                    row[venue+'_ask']=book['asks'][0][0]
                row['gross_cross_edge']=max(row['kraken_bid']/row['coinbase_ask']-1,row['coinbase_bid']/row['kraken_ask']-1)
                row['reason']='Depth and configured fees screened; account capacity, snapshot timing, queue fills and later-period replay remain unverified'
            except Exception as exc:
                row['depth_error']=type(exc).__name__
        return sorted(reports,key=lambda r:((r.get('maker_depth_routes') or [{}])[0].get('indicative_net_edge',-math.inf),r['min_venue_quote_volume_24h']),reverse=True)
    finally:
        await asyncio.gather(*(ex.close() for ex in exchanges.values()),return_exceptions=True)
