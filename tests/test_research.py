import asyncio
import copy
import json
import sqlite3
import tempfile
import time
import unittest
import subprocess
import sys
from pathlib import Path
from unittest.mock import AsyncMock
import ccxt
from src.configuration import load_config,fingerprint
from src.economics import break_even,economics_report,save_receipts,AccountInspector
from src.operations import backup_database,watchdog_check,atomic_json
from src.portfolio import PortfolioRisk
from src.recorder import Recorder,read_events,sessions
from src.recovery import OrderLedger,DisconnectGuard
from src.replay import MakerReplay,walk_forward
from src.discovery import depth_route
from src.feeds import public_trade_payload


class ResearchTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name)
        self.cfg=load_config()
        self.cfg['arb_groups']={'BTC':{'coinbase':'BTC/USDT','kraken':'BTC/USDT'}}
        self.cfg['triangles']={}
        self.cfg['scanner']['stale_book_ms']=10000
        self.cfg['scanner']['max_book_skew_ms']=10000
        self.cfg['scanner']['slippage_buffer']=0
        self.cfg['scanner']['rebalance_buffer']=0
        self.cfg['research']['max_unhedged_usdt']=.01
        self.cfg['risk']['cooldown_s']=10
        self.cfg['recording']['db_path']=str(self.root/'tape.db')
        self.clock=1000
        self.seq=0

    def event(self,kind,venue='',symbol='',body=None,t=None,source=None):
        self.seq+=1
        if kind=='trade':body={'side_basis':'taker',**(body or {})}
        return dict(id=self.seq,received=self.clock if t is None else t,kind=kind,venue=venue,symbol=symbol,
                    source_ts=source,body=body or {})

    def market(self,venue):
        return self.event('markets',venue,body={'precision_mode':ccxt.TICK_SIZE,'markets':{'BTC/USDT':dict(
            symbol='BTC/USDT',id='BTCUSDT',spot=True,active=True,base='BTC',quote='USDT',
            precision={'amount':.000001,'price':.01},limits={'amount':{'min':.000001},'cost':{'min':.001}})}})

    def sim(self,**kwargs):
        sim=MakerReplay(self.cfg,entry_delay_ms=100,hedge_delay_ms=200,quote_lifetime_ms=2000,**kwargs)
        for v in ('coinbase','kraken'):sim.consume(self.market(v))
        sim.consume(self.event('book','coinbase','BTC/USDT',{'bids':[[100,.1]],'asks':[[101,50]]}))
        sim.consume(self.event('book','kraken','BTC/USDT',{'bids':[[110,50]],'asks':[[111,50]]}))
        return sim

    def trade(self,sim,qty=.2,t=1000.2,source=None):
        sim.consume(self.event('trade','coinbase','BTC/USDT',dict(side='sell',price=100,amount=qty),t=t,source=t if source is None else source))

    def test_break_even_exact_fee_compounding(self):
        edge=break_even(.01,.02,.001,.003)
        buy=100*(1.001)*1.01
        sell=100*(1+edge)*.999*.98
        self.assertAlmostEqual(sell,buy*1.003)

    def test_economics_never_labels_estimates_verified(self):
        report=economics_report(self.cfg)
        self.assertFalse(report['account_verified'])
        self.assertTrue(all(not r['fees_verified'] for r in report['rows']))
        self.assertEqual(report['rows'][0]['minimum_committed_capital_usdt'],50)

    def test_receipts_reject_actual_cost_increase(self):
        db=sqlite3.connect(':memory:');self.addCleanup(db.close)
        account={'coinbase':{'verified_at':time.time(),'fees':{'BTC/USDT':{'maker':.9,'taker':.9}}}}
        with self.assertRaisesRegex(ValueError,'exceeds configuration'):save_receipts(db,self.cfg,account)

    def test_recording_roundtrip_and_session_closed(self):
        recorder=Recorder(self.root/'tape.db',self.cfg,fingerprint(self.cfg))
        self.assertTrue(recorder.emit('book','coinbase','BTC/USDT',{'bids':[[100,2]],'asks':[[101,2]]},source_ts=10))
        recorder.close()
        events=list(read_events(self.root/'tape.db',recorder.session))
        self.assertEqual(events[-1]['body']['bids'],[[100,2]])
        self.assertIsNotNone(sessions(self.root/'tape.db')[0]['ended'])

    def test_invalid_event_marks_tape_unhealthy(self):
        recorder=Recorder(self.root/'tape.db',self.cfg,fingerprint(self.cfg))
        self.assertFalse(recorder.emit('book',body={'price':float('nan')}))
        self.assertFalse(recorder.snapshot()['healthy'])
        recorder.close()
        self.assertEqual(list(read_events(self.root/'tape.db',recorder.session))[-1]['kind'],'recording_gap')

    def test_maker_waits_for_activation_and_queue(self):
        sim=self.sim()
        self.trade(sim,qty=.1,t=1000.05)
        self.assertEqual(sim.fills,0)
        self.trade(sim,qty=.1,t=1000.2)
        self.assertEqual(sim.fills,0)
        self.trade(sim,qty=.05,t=1000.21)
        self.assertEqual(sim.fills,1)
        self.assertAlmostEqual(sim.filled_qty,.05)

    def test_old_trade_cannot_fill_new_order(self):
        sim=self.sim()
        self.trade(sim,qty=10,t=1000.2,source=999)
        self.assertEqual(sim.fills,0)

    def test_trade_without_timestamp_cannot_fill(self):
        sim=self.sim()
        sim.consume(self.event('trade','coinbase','BTC/USDT',dict(side='sell',price=100,amount=10),t=1000.2))
        self.assertEqual(sim.fills,0)

    def test_hedge_uses_book_before_future_event(self):
        sim=self.sim()
        self.trade(sim,qty=.2,t=1000.2)
        # Hedge due at .4 executes at 110; the .5 update must not be visible to it.
        sim.consume(self.event('book','kraken','BTC/USDT',{'bids':[[50,50]],'asks':[[51,50]]},t=1000.5))
        hedge=[x for x in sim.log if x['type']=='hedge'][0]
        self.assertAlmostEqual(hedge['filled'],.1)
        self.assertGreater(sim.balances['kraken']['USDT'],250+10)

    def test_delayed_hedge_sees_intervening_adverse_move(self):
        sim=self.sim()
        self.trade(sim,qty=.2,t=1000.2)
        sim.consume(self.event('book','kraken','BTC/USDT',{'bids':[[80,50]],'asks':[[81,50]]},t=1000.3))
        sim.consume(self.event('scan',t=1000.5))
        self.assertLess(sim.balances['kraken']['USDT'],250+9)

    def test_partial_hedge_halts_and_records_exposure(self):
        sim=self.sim()
        self.trade(sim,qty=.2,t=1000.2)
        sim.consume(self.event('book','kraken','BTC/USDT',{'bids':[[110,.03]],'asks':[[111,50]]},t=1000.3))
        sim.consume(self.event('scan',t=1000.5))
        self.assertTrue(sim.stopped)
        self.assertGreater(sim.report()['unhedged_usdt'],0)

    def test_same_depth_cannot_be_consumed_twice(self):
        sim=self.sim()
        sim.books[('kraken','BTC/USDT')]['bids']=[[110,.1]]
        sim.now=1000.2;sim.unmatched={'BTC':.2}
        sim.hedge(dict(venue='kraken',symbol='BTC/USDT',qty=.1,cost=10))
        sim.hedge(dict(venue='kraken',symbol='BTC/USDT',qty=.1,cost=10))
        self.assertAlmostEqual(sim.hedged_qty,.1)
        self.assertTrue(sim.stopped)

    def test_halt_keeps_resting_orders_exposed_until_cancel_ack(self):
        sim=self.sim(cancel_delay_ms=500)
        sim.consume(self.event('scan',t=1000.2))
        sim.halt('Test hedge outage')
        self.trade(sim,qty=.15,t=1000.3)
        self.assertEqual(sim.fills,1)
        sim.consume(self.event('scan',t=1000.8))
        self.trade(sim,qty=10,t=1000.9)
        self.assertEqual(sim.fills,1)
        self.assertFalse(sim.orders)

    def test_duplicate_public_trade_only_fills_once(self):
        sim=self.sim()
        for t in (1000.2,1000.21):
            sim.consume(self.event('trade','coinbase','BTC/USDT',dict(id='same',side='sell',price=100,amount=.15),t=t,source=1000.2))
        self.assertAlmostEqual(sim.filled_qty,.05)

    def test_coinbase_public_trade_maker_side_is_inverted(self):
        payload=public_trade_payload('coinbase',dict(side='buy',info={'side':'BUY'},price=100,amount=.15))
        self.assertEqual(payload['side'],'sell')
        sim=self.sim()
        sim.consume(self.event('trade','coinbase','BTC/USDT',payload,t=1000.2,source=1000.2))
        self.assertAlmostEqual(sim.filled_qty,.05)

    def test_legacy_coinbase_tape_uses_maker_side(self):
        sim=self.sim()
        e=self.event('trade','coinbase','BTC/USDT',dict(side='buy',price=100,amount=.15),t=1000.2,source=1000.2)
        del e['body']['side_basis']
        sim.consume(e)
        self.assertAlmostEqual(sim.filled_qty,.05)

    def test_invalid_book_invalidates_cached_liquidity(self):
        sim=self.sim()
        sim.consume(self.event('book','kraken','BTC/USDT',{'bids':[[float('nan'),1]],'asks':[[111,50]]},t=1000.3))
        self.assertFalse(sim.fresh('kraken','BTC/USDT'))
        self.assertFalse(sim.report()['data_integrity_ok'])

    def test_nonfinite_replay_parameters_rejected(self):
        with self.assertRaises(ValueError):MakerReplay(self.cfg,hedge_delay_ms=float('nan'))

    def test_account_capacity_uses_free_inventory_and_cash(self):
        books={'BTC/USDT':dict(bids=[[100,20]],asks=[[101,20]],received=time.time())}
        accounts={v:dict(fees={'BTC/USDT':dict(maker=.001,taker=.002)},books=books,
            balances={'USDT':dict(free=10,total=100),'BTC':dict(free=.01,total=1)}) for v in ('coinbase','kraken')}
        report=economics_report(self.cfg,accounts)
        self.assertTrue(report['account_verified'])
        self.assertTrue(all(r['indicative_capacity_base_qty']==.01 for r in report['rows']))
        self.assertFalse(economics_report(self.cfg,{'coinbase':accounts['coinbase']})['account_verified'])

    def test_operating_cost_sensitivity_raises_break_even(self):
        self.cfg['economics']['operating_cost_usdt_per_day']=5
        row=economics_report(self.cfg)['rows'][0]
        self.assertGreater(row['operating_cost_sensitivity'][0]['break_even_gross_edge'],row['break_even_gross_edge'])
        self.assertGreater(row['operating_cost_sensitivity'][0]['break_even_gross_edge'],row['operating_cost_sensitivity'][-1]['break_even_gross_edge'])

    def test_discovery_accepts_kraken_rest_level_timestamps(self):
        sim=self.sim()
        books={'coinbase':dict(bids=[[100,10]],asks=[[101,10]]),
               'kraken':dict(bids=[[110,10,1788794000]],asks=[[111,10,1788794000]])}
        result=depth_route(self.cfg,sim.adapters,'BTC/USDT','coinbase','kraken',books)
        self.assertGreater(result['indicative_net_profit_quote'],0)

    def test_disconnect_guard_arms_milliseconds_and_stops_on_kill(self):
        async def scenario():
            ex=AsyncMock();ex.has={'cancelAllOrdersAfter':True}
            reasons=[];kill=self.root/'KILL'
            guard=DisconnectGuard(ex,reasons.append,kill,lambda:True,renew_s=.001)
            await guard.arm()
            kill.touch()
            await asyncio.wait_for(guard.run(),1)
            ex.cancel_all_orders_after.assert_awaited_once_with(60000)
            self.assertTrue(reasons)
        asyncio.run(scenario())

    def test_disconnect_guard_failure_latches_halt(self):
        async def scenario():
            ex=AsyncMock();ex.has={'cancelAllOrdersAfter':True}
            ex.cancel_all_orders_after.side_effect=[{},RuntimeError('network outage')]
            reasons=[]
            guard=DisconnectGuard(ex,reasons.append,self.root/'KILL',lambda:True,renew_s=.001)
            await guard.arm()
            await asyncio.wait_for(guard.run(),1)
            self.assertIn('failed',reasons[0])
        asyncio.run(scenario())

    def test_watchdog_process_latches_without_engine_cooperation(self):
        hb=self.root/'heartbeat.json';kill=self.root/'KILL'
        atomic_json(hb,dict(run_id='drill',ts=time.time()-100,status='running',recording_healthy=True))
        result=subprocess.run([sys.executable,'watchdog.py','--heartbeat',str(hb),'--kill-file',str(kill),
            '--run-id','drill','--startup-grace','0'],capture_output=True,timeout=5)
        self.assertEqual(result.returncode,0,result.stderr)
        self.assertTrue(kill.exists())
        self.assertIn('expired',json.loads((self.root/'watchdog-alert.json').read_text())['reason'])

    def test_order_intent_survives_abrupt_process_exit(self):
        path=self.root/'crash.db'
        code="""
import os,sqlite3,sys
db=sqlite3.connect(sys.argv[1])
db.execute('PRAGMA journal_mode=WAL')
db.execute('PRAGMA synchronous=FULL')
db.execute('CREATE TABLE live_orders(client_id TEXT,ts REAL,venue TEXT,symbol TEXT,side TEXT,qty REAL,order_id TEXT,status TEXT)')
db.execute("INSERT INTO live_orders VALUES('crash',100,'coinbase','BTC/USDT','buy',1,NULL,'intent')")
db.commit()
os._exit(73)
"""
        result=subprocess.run([sys.executable,'-c',code,str(path)],capture_output=True,timeout=5)
        self.assertEqual(result.returncode,73)
        with sqlite3.connect(path) as db:
            ledger=OrderLedger(db)
            ex=AsyncMock();ex.fetch_open_orders.return_value=[];ex.fetch_closed_orders.return_value=[]
            audit=asyncio.run(ledger.audit({'coinbase':ex}))
            self.assertEqual(audit['unresolved'],1)
            self.assertFalse(audit['halt_cleared'])
            ex.create_order.assert_not_called()

    def test_cancellation_latency_exposes_order_to_fills(self):
        sim=self.sim(cancel_delay_ms=500)
        sim.consume(self.event('feed_error','coinbase','BTC/USDT',t=1000.2))
        self.trade(sim,qty=.2,t=1000.3)
        self.assertEqual(sim.fills,1)

    def test_old_cancel_does_not_cancel_replacement(self):
        sim=self.sim()
        key=('coinbase','BTC/USDT')
        old=sim.orders[key]
        replacement=dict(old)
        sim.schedule(1000.3,'cancel',old)
        sim.orders[key]=replacement
        sim.process_due(1000.4)
        self.assertIs(sim.orders[key],replacement)

    def test_recording_gap_stops_replay(self):
        sim=self.sim()
        sim.consume(self.event('recording_gap',t=1000.1))
        self.assertTrue(sim.stopped)

    def test_walk_forward_refuses_open_recording(self):
        recorder=Recorder(self.root/'tape.db',self.cfg,fingerprint(self.cfg))
        try:
            with self.assertRaisesRegex(ValueError,'Close the recording'):walk_forward(self.root/'tape.db',recorder.session)
        finally:recorder.close()

    def test_portfolio_requires_all_inventory_prices(self):
        db=sqlite3.connect(':memory:');self.addCleanup(db.close)
        risk=PortfolioRisk(self.cfg,db)
        reasons=risk.check({'coinbase':{'USDT':500,'BTC':1}},{})
        self.assertTrue(any('Unpriced' in s for s in reasons))

    def test_portfolio_blocks_drawdown_and_concentration(self):
        db=sqlite3.connect(':memory:');self.addCleanup(db.close)
        risk=PortfolioRisk(self.cfg,db)
        risk.mark({'coinbase':{'USDT':500,'BTC':1}},{'BTC':100})
        self.assertTrue(any('drawdown' in s for s in risk.check({'coinbase':{'USDT':400,'BTC':1}},{'BTC':50})))
        self.assertTrue(any('exposure' in s for s in risk.check({'coinbase':{'USDT':500,'BTC':10}},{'BTC':100})))

    def test_sqlite_backup_preserves_rows_and_integrity(self):
        source=self.root/'source.db';target=self.root/'backup.db'
        with sqlite3.connect(source) as db:
            db.execute('CREATE TABLE x(value INTEGER)');db.execute('INSERT INTO x VALUES(42)')
        self.assertEqual(backup_database(source,target)['integrity'],'ok')
        with sqlite3.connect(target) as db:self.assertEqual(db.execute('SELECT * FROM x').fetchone()[0],42)
        with self.assertRaises(ValueError):backup_database(source,target)

    def test_watchdog_detects_stale_wrong_run_and_recorder_gap(self):
        hb=dict(run_id='a',ts=100,status='running',recording_healthy=True)
        self.assertIsNone(watchdog_check(hb,105,10,'a'))
        self.assertIsNotNone(watchdog_check(hb,120,10,'a'))
        self.assertIsNotNone(watchdog_check(hb,105,10,'b'))
        hb['recording_healthy']=False
        self.assertIsNotNone(watchdog_check(hb,105,10,'a'))

    def test_order_ledger_rejects_decreasing_fill(self):
        db=sqlite3.connect(':memory:');self.addCleanup(db.close)
        ledger=OrderLedger(db)
        ledger.observe('c',{'id':'1','filled':1,'status':'open'},'rest')
        with self.assertRaisesRegex(ValueError,'decreased'):
            ledger.observe('c',{'id':'1','filled':.5,'status':'open'},'ws')

    def test_late_open_event_does_not_reopen_closed_order(self):
        db=sqlite3.connect(':memory:');self.addCleanup(db.close)
        ledger=OrderLedger(db)
        ledger.observe('c',{'id':'1','filled':1,'status':'closed'},'rest')
        ledger.observe('c',{'id':'1','filled':1,'status':'open'},'ws')
        self.assertEqual(db.execute('SELECT status FROM order_observations').fetchone()[0],'closed')

    def test_unknown_order_audit_never_resubmits(self):
        db=sqlite3.connect(':memory:');self.addCleanup(db.close)
        db.execute('CREATE TABLE live_orders(client_id TEXT,ts REAL,venue TEXT,symbol TEXT,side TEXT,qty REAL,order_id TEXT,status TEXT)')
        db.execute("INSERT INTO live_orders VALUES('c',100,'coinbase','BTC/USDT','buy',1,NULL,'intent')")
        ledger=OrderLedger(db)
        ex=AsyncMock();ex.fetch_open_orders.return_value=[];ex.fetch_closed_orders.return_value=[]
        result=asyncio.run(ledger.audit({'coinbase':ex}))
        self.assertEqual(result['unresolved'],1)
        self.assertFalse(result['halt_cleared'])
        ex.create_order.assert_not_called()


if __name__=='__main__':unittest.main()
