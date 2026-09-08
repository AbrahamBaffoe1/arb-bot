"""Market data -> depth pricing -> persistent confirmation -> risk -> execution."""
from __future__ import annotations
import asyncio
import logging
import time
import json
import os
import subprocess
import sys
import shutil
from dataclasses import asdict
from pathlib import Path
from .configuration import ROOT,fingerprint
from .recorder import Recorder
from .operations import Operations,backup_database
from .portfolio import PortfolioRisk
from .evidence import Evidence
from .feeds import BookStore, FeedManager
from .paper import PaperExecutor
from .risk import RiskManager
from .scanners import CrossExchangeScanner, TriangularScanner

log = logging.getLogger('engine')


class Engine:
    def __init__(self, cfg):
        self.cfg = cfg
        self.store = BookStore()
        self.feeds = FeedManager(cfg, self.store)
        self.cross = CrossExchangeScanner(cfg, self.store)
        self.tri = TriangularScanner(cfg, self.store)
        self.paper = PaperExecutor(cfg)
        self.executor = self.paper
        self.evidence = Evidence(cfg, self.paper.db)
        self.risk = RiskManager(cfg)
        self.recent_opps, self.equity_curve, self.route_estimates = [], [], []
        self.started, self.scans = time.time(), 0
        self.marks = {}
        self._funding_announced = False
        self.status = 'starting'
        self.recorder=None
        self.operations=None
        self.portfolio=PortfolioRisk(cfg,self.paper.db)
        self.watchdog=None
        self.portfolio_report={}
        self.last_operating_charge=time.time()
        self._backup_task=None

    async def backup(self):
        stamp=time.strftime('%Y%m%d-%H%M%S',time.gmtime())
        folder=Path(self.cfg['operations']['backup_dir'])/stamp
        sources=[self.cfg['storage']['db_path'],self.cfg['recording']['db_path']]
        if self.cfg['mode']=='live':sources.append(self.cfg['storage']['live_db_path'])
        try:
            backup_root=Path(self.cfg['operations']['backup_dir'])
            used=sum(p.stat().st_size for p in backup_root.rglob('*') if p.is_file()) if backup_root.exists() else 0
            required=sum(Path(p).stat().st_size+sum(w.stat().st_size for w in [Path(p+'-wal')] if w.exists()) for p in sources)
            budget=self.cfg['operations'].get('max_backup_disk_gb',24)*1024**3
            reserve=self.cfg['recording'].get('min_free_disk_gb',10)*1024**3
            if used+required>budget or shutil.disk_usage(ROOT/'data').free-required<reserve:
                raise RuntimeError('Backup capacity reached; archive existing backups to separate storage')
            for source in sources:
                await asyncio.to_thread(backup_database,source,folder/Path(source).name)
            self.operations.alert('backup_complete',str(folder),'info')
        except Exception as exc:
            self.operations.alert('backup_failed',str(exc),'critical')

    def _prices(self):
        for group, mapping in self.cfg['arb_groups'].items():
            for venue, symbol in mapping.items():
                if self.store.usable(venue, symbol, self.cfg['scanner']['stale_book_ms']):
                    book = self.store.get(venue, symbol)
                    self.marks[group] = (book.best_bid + book.best_ask) / 2
                    break
        return dict(self.marks)

    async def run(self):
        if self.cfg['mode'] == 'live':
            funding = self.evidence.funding()
            if not funding['ready']:
                raise ValueError('Live mode blocked: ' + '; '.join(funding['blockers']))
            from .live import LiveExecutor
            self.executor = LiveExecutor(self.cfg)
            await self.executor.initialize()
            self.executor.start_private_streams()
            await self.executor.start_disconnect_guard()
            self.portfolio=self.executor.portfolio
        self.recorder=Recorder(self.cfg['recording']['db_path'],self.cfg,fingerprint(self.cfg))
        self.feeds.recorder=self.recorder
        self.operations=Operations(self.cfg,self.executor.db,self.recorder.session)
        if self.cfg['mode']=='live': self.executor.telemetry=self.operations
        if self.cfg['operations'].get('watchdog_enabled',True):
            self.watchdog=subprocess.Popen([sys.executable,str(ROOT/'watchdog.py'),
                '--heartbeat',self.cfg['operations']['heartbeat_file'],'--kill-file',self.cfg['risk']['kill_switch_file'],
                '--run-id',self.recorder.session,'--max-age',str(self.cfg['operations']['watchdog_max_age_s'])],cwd=ROOT)
        await self.feeds.start()
        self.status = 'running'
        interval = self.cfg['scanner']['interval_ms'] / 1000
        last_sample = 0.0
        last_equity = 0.0
        last_fee_check = time.time()
        last_backup=time.time()
        while True:
            t0 = time.time()
            candidates = self.cross.candidates()
            opps = [o for o in candidates if self.cross.qualifies(o)] + self.tri.scan()
            self.scans += 1
            prices = self._prices()
            self.operations.observe('scan_compute_ms',(time.time()-t0)*1000)
            if self.watchdog and self.watchdog.poll() is not None:
                self.risk.kill_file.touch(exist_ok=True)
                self.operations.alert('watchdog_exited','Independent watchdog exited; new execution is latched off','critical')
            self.risk.check_halt(self.executor.risk_pnl_today())
            if self.cfg['mode'] == 'paper' and not self.risk.halted_reason:
                self.paper.seed_inventory(self.store)
            healthy = bool(candidates)
            # All held assets need current marks before taking additional inventory risk.
            priced_assets = {group for group, mapping in self.cfg['arb_groups'].items()
                             if any(self.store.usable(v, s, self.cfg['scanner']['stale_book_ms']) for v, s in mapping.items())}
            held_assets = {a for b in self.executor.balances.values() for a, q in b.items() if a != 'USDT' and q > 0}
            recording_healthy=self.recorder.snapshot()['healthy']
            lag=self.operations.tick(self.cfg['mode'],healthy,recording_healthy,
                                     getattr(self.executor,'halted_reason',None) or self.risk.halted_reason)
            for metric in self.feeds.metrics.values():
                if metric['source_age_ms'] is not None:
                    self.operations.observe('book_source_age_ms',metric['source_age_ms'])
            self.recorder.emit('scan',body={'candidates':len(candidates),'qualifying':len(opps),
                                           'fresh_inventory':held_assets<=priced_assets,'halted':self.risk.halted_reason})
            if self.cfg['mode'] == 'live' and t0 - last_fee_check >= 900:
                try:
                    await self.executor.verify_fees()
                except Exception:
                    self.executor.halt('Account fee verification failed or fee tier increased')
                last_fee_check = time.time()
                continue  # rescan after private network calls before using any quote
            for window_id, opp in self.evidence.update(opps, healthy and recording_healthy and self.cfg['mode'] == 'paper'):
                status, reason, fill = 'rejected', '', None
                projected=self.executor.project(opp)
                portfolio_reasons=self.portfolio.check(projected,prices,self.executor.baseline) if projected else ['Insufficient funded inventory']
                if not recording_healthy:
                    reason='Market recorder is unhealthy'
                elif lag>self.cfg['operations']['max_event_loop_lag_ms']:
                    reason='Event loop latency exceeded limit'
                elif portfolio_reasons:
                    reason='; '.join(portfolio_reasons)
                elif not held_assets <= priced_assets:
                    reason = 'fresh inventory marks unavailable'
                elif self.risk.allow(opp, self.executor.risk_pnl_today(), self.executor.inventory_skew_usdt(prices)):
                    if self.cfg['mode'] == 'live':
                        fill = await self.executor.execute_live(opp, self.store, prices)
                    else:
                        fill = self.executor.execute(opp, prices)
                    reason = self.executor.last_rejection or getattr(self.executor, 'halted_reason', None) or ''
                    if fill:
                        self.risk.record_fill(opp.group)
                        status = 'paper_filled' if self.cfg['mode'] == 'paper' else 'live_filled'
                else:
                    reason = self.risk.halted_reason or 'risk limit or cooldown'
                self.evidence.result(window_id, status, reason, fill.pnl if fill else None)
                self.recorder.emit('decision',body={'window_id':window_id,'plan':asdict(opp),'outcome':status,
                                                     'reason':reason,'pnl':fill.pnl if fill else None})
                self.recent_opps.insert(0, dict(ts=opp.ts, kind=opp.kind, group=opp.group, detail=opp.detail,
                                              net_edge=opp.net_edge, net_profit=opp.net_profit, status=status, reason=reason))
                del self.recent_opps[100:]
            if t0 - last_sample >= 1:
                best = {}
                for o in candidates:
                    if o.group not in best or o.net_profit > best[o.group].net_profit:
                        best[o.group] = o
                self.route_estimates = [dict(group=o.group, buy_venue=o.buy_venue, sell_venue=o.sell_venue,
                                            gross=o.gross_edge, net_edge=o.net_edge, net_profit=o.net_profit,
                                            notional=o.notional) for o in best.values()]
                self.paper.record_spreads(t0, self.route_estimates)
                funding = self.evidence.funding(self.risk.halted_reason)
                if self.cfg['mode'] == 'paper' and funding['ready'] and not self._funding_announced:
                    log.warning('%s Proposal: %s', funding['message'], funding['proposal'])
                self._funding_announced = funding['ready']
                last_sample = t0
            if t0 - last_equity >= 5 and prices:
                if held_assets <= priced_assets:
                    self.executor.mark_equity(prices)
                    self.portfolio_report=self.portfolio.mark(self.executor.balances,prices,self.executor.baseline)
                    self.portfolio_report['available_hedge_inventory_qty']={v:{a:q for a,q in h.items() if a!='USDT'}
                        for v,h in getattr(self.executor,'free_balances',self.executor.balances).items()}
                    self.portfolio_report['rebalance_proposals']=self.portfolio.rebalance_plan(self.executor.balances,prices,self.executor.baseline)
                operating=self.cfg.get('economics',{}).get('operating_cost_usdt_per_day',0)*(t0-self.last_operating_charge)/86400
                if operating>0 and self.cfg['mode']=='paper':
                    with self.paper.db:
                        self.paper.db.execute('INSERT INTO expenses VALUES(?,?,?)',(t0,operating,'operating costs'))
                        self.paper.realized_pnl-=operating
                        venue=next(iter(self.paper.balances))
                        self.paper.balances[venue]['USDT']-=operating
                        self.paper._persist()
                self.last_operating_charge=t0
                self.equity_curve.append([t0, round(self.executor.total_equity(prices), 2)])
                del self.equity_curve[:-2000]
                last_equity = t0
            if t0-last_backup>=self.cfg['operations']['backup_interval_hours']*3600 and (self._backup_task is None or self._backup_task.done()):
                self._backup_task=asyncio.create_task(self.backup())
                last_backup=t0
            await asyncio.sleep(max(0, interval - (time.time() - t0)))

    def snapshot(self):
        prices = self._prices()
        stale = self.cfg['scanner']['stale_book_ms']
        spreads = []
        for group, mapping in self.cfg['arb_groups'].items():
            venues = {v: dict(bid=self.store.get(v, s).best_bid, ask=self.store.get(v, s).best_ask)
                      for v, s in mapping.items() if self.store.usable(v, s, stale)}
            row = dict(group=group, venues=venues)
            if len(venues) >= 2:
                row['spread'] = max((sell['bid'] - buy['ask']) / buy['ask']
                                    for va, buy in venues.items() for vb, sell in venues.items() if va != vb)
            spreads.append(row)
        statuses = {}
        for v, state in self.store.venue_status.items():
            statuses[v] = 'live' if any(self.store.usable(v, s, stale) for s in self.store.books.get(v, {})) else ('stale' if state == 'live' else state)
        halt = getattr(self.executor, 'halted_reason', None) or self.risk.halted_reason
        funding = self.evidence.funding(halt)
        if self.cfg['mode'] == 'live':
            funding.update(status='live_halted' if halt else 'live_pilot', ready=False,
                           message=f'Live pilot halted: {halt}' if halt else 'Live pilot enabled. Monitor confirmed fills and exchange inventory.')
        return dict(ts=time.time(), mode=self.cfg['mode'], uptime_s=time.time()-self.started,
                    recording=self.recorder.snapshot() if self.recorder else {'healthy':False,'error':'starting'},
                    operations=self.operations.snapshot() if self.operations else {},portfolio=self.portfolio_report,
                    private_streams=getattr(self.executor,'private_status',{}),
                    scans=self.scans, status=self.status, venue_status=statuses,
                    symbol_status=self.store.symbol_status, prices=prices, spreads=spreads,
                    route_estimates=self.route_estimates, opportunities=self.recent_opps[:30],
                    fills=[dict(ts=f.ts, kind=f.kind, detail=f.detail, notional=f.notional, pnl=f.pnl)
                           for f in self.executor.fills[-50:]][::-1],
                    balances=self.executor.balances, realized_pnl=self.executor.realized_pnl,
                    pnl_today=self.executor.pnl_today(), equity=self.executor.total_equity(prices),
                    equity_curve=self.equity_curve[-500:], halted=halt, funding=funding)

    async def close(self):
        self.status = 'stopped'
        await self.feeds.close()
        if self._backup_task:
            await self._backup_task
        if self.executor is not self.paper:
            await self.executor.aclose()
        if self.recorder:
            await asyncio.to_thread(self.recorder.close)
        if self.operations:
            self.operations.stopped()
        if self.watchdog:
            try:
                await asyncio.wait_for(asyncio.to_thread(self.watchdog.wait),timeout=3)
            except asyncio.TimeoutError:
                self.watchdog.terminate()
                await asyncio.to_thread(self.watchdog.wait)
        self.paper.close()
