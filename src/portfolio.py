"""Portfolio valuation and pre-trade exposure checks across both exchange accounts."""
from __future__ import annotations
import math
import time


class PortfolioRisk:
    def __init__(self,cfg,db):
        self.cfg=cfg
        self.settings=cfg.get('portfolio',{})
        self.db=db
        db.execute('CREATE TABLE IF NOT EXISTS portfolio_state(id INTEGER PRIMARY KEY CHECK(id=1), initial REAL, peak REAL, last REAL, ts REAL)')
        db.commit()

    def snapshot(self,balances,prices,baseline=None):
        missing=sorted({a for assets in balances.values() for a,q in assets.items() if q>0 and a!='USDT'
                        and (a not in prices or not math.isfinite(prices[a]) or prices[a]<=0)})
        venues={}
        assets={}
        cash=0.0
        for venue,holdings in balances.items():
            value=0.0
            for asset,qty in holdings.items():
                worth=qty if asset=='USDT' else qty*(prices.get(asset,0) if asset not in missing else 0)
                value+=worth
                if asset=='USDT':
                    cash+=qty
                else:
                    assets[asset]=assets.get(asset,0)+worth
            venues[venue]=value
        equity=sum(venues.values())
        state=self.db.execute('SELECT initial,peak,last,ts FROM portfolio_state WHERE id=1').fetchone()
        peak=max(equity,state[1] if state else equity)
        initial=state[0] if state else (self.cfg['paper']['starting_balance_usdt']*len(balances) if self.cfg['mode']=='paper' else equity)
        drift={}
        for venue,holdings in balances.items():
            drift[venue]=sum(abs(q-(baseline or {}).get(venue,{}).get(a,0))*prices.get(a,0)
                             for a,q in holdings.items() if a!='USDT')
        return dict(equity_usdt=equity,committed_capital_usdt=initial,cash_usdt=cash,
                    cash_fraction=cash/equity if equity else 0,asset_exposure_usdt=assets,
                    cash_by_venue_usdt={v:h.get('USDT',0) for v,h in balances.items()},
                    hedge_inventory_qty={v:{a:q for a,q in h.items() if a!='USDT'} for v,h in balances.items()},
                    venue_exposure_usdt=venues,inventory_drift_usdt=drift,unpriced_assets=missing,
                    peak_equity_usdt=peak,drawdown_fraction=(peak-equity)/peak if peak else 0,
                    return_on_committed_capital=(equity-initial)/initial if initial else 0)

    def mark(self,balances,prices,baseline=None):
        report=self.snapshot(balances,prices,baseline)
        if report['unpriced_assets']:
            return report
        with self.db:
            self.db.execute('INSERT INTO portfolio_state VALUES(1,?,?,?,?)'+
                ' ON CONFLICT(id) DO UPDATE SET peak=excluded.peak,last=excluded.last,ts=excluded.ts',
                (report['committed_capital_usdt'],report['peak_equity_usdt'],report['equity_usdt'],time.time()))
        return report

    def check(self,balances,prices,baseline=None):
        report=self.snapshot(balances,prices,baseline)
        reasons=[]
        if report['unpriced_assets']:
            reasons.append('Unpriced inventory: '+', '.join(report['unpriced_assets']))
        if any(not math.isfinite(q) or q<0 for h in balances.values() for q in h.values()):
            reasons.append('Invalid or negative account inventory')
        if report['drawdown_fraction']>self.settings.get('max_drawdown_fraction',.05):
            reasons.append('Portfolio drawdown limit reached')
        if report['cash_fraction']<self.settings.get('min_cash_fraction',.2):
            reasons.append('Portfolio cash reserve below limit')
        cap=self.settings.get('max_asset_exposure_usdt',200)
        if any(v>cap for v in report['asset_exposure_usdt'].values()):
            reasons.append('Asset exposure limit exceeded')
        venue_cap=self.settings.get('max_venue_exposure_usdt',650)
        if any(v>venue_cap for v in report['venue_exposure_usdt'].values()):
            reasons.append('Exchange exposure limit exceeded')
        return reasons

    def rebalance_plan(self,balances,prices,baseline):
        rows=[]
        for venue,holdings in balances.items():
            for asset,qty in holdings.items():
                if asset=='USDT' or asset not in prices:
                    continue
                delta=qty-baseline.get(venue,{}).get(asset,0)
                if abs(delta)*prices[asset]<self.settings.get('rebalance_trigger_usdt',25):
                    continue
                rows.append(dict(venue=venue,asset=asset,side='sell' if delta>0 else 'buy',qty=abs(delta),
                                 indicative_notional_usdt=abs(delta)*prices[asset],automatic=False))
        return rows
