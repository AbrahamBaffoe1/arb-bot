import asyncio
import copy
import json
import math
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch
import ccxt
from src.configuration import load_config, validate
from src.evidence import Evidence
from src.feeds import BookStore
from src.live import LiveExecutor
from src.locking import ProcessLock
from src.models import Book
from src.paper import PaperExecutor
from src.risk import RiskManager
from src.scanners import CrossExchangeScanner, TriangularScanner


class BaseFixture(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.cfg = load_config()
        self.cfg['storage'] = {'db_path': str(Path(self.tmp.name)/'paper.db'), 'live_db_path': str(Path(self.tmp.name)/'live.db')}
        self.cfg['risk']['kill_switch_file'] = str(Path(self.tmp.name)/'KILL')
        self.cfg['arb_groups'] = {'BTC': {'coinbase': 'BTC/USDT', 'kraken': 'BTC/USDT'}}
        self.cfg['triangles'] = {}
        self.cfg['scanner']['sizes_usdt'] = [25]
        self.cfg['risk']['max_open_skew_usdt'] = 1000
        self.store = BookStore()
        for venue in self.cfg['venues']:
            ex = getattr(ccxt, venue)()
            market = dict(symbol='BTC/USDT', spot=True, active=True, id='BTCUSDT',
                          precision={'amount': .000001, 'price': .0001},
                          limits={'amount': {'min': .00001}, 'cost': {'min': 1}})
            ex.markets = {'BTC/USDT': market}
            self.store.markets[venue] = ex.markets
            self.store.exchanges[venue] = ex
            self.store.venue_status[venue] = 'live'
        self.books()
        self.scanner = CrossExchangeScanner(self.cfg, self.store)

    def books(self):
        self.store.update('coinbase', 'BTC/USDT', [(99, 20)], [(100, 20)])
        self.store.update('kraken', 'BTC/USDT', [(105, 20)], [(106, 20)])

    def paper(self):
        ex = PaperExecutor(self.cfg)
        self.addCleanup(ex.close)
        return ex

    def opportunity(self):
        return next(o for o in self.scanner.scan() if o.buy_venue == 'coinbase')


class PricingTests(BaseFixture):
    def test_exact_cash_profit_and_precision(self):
        o = self.opportunity()
        self.assertAlmostEqual(o.net_profit, o.legs[1]['cash']-o.legs[0]['cash']-o.reserve)
        self.assertLessEqual(o.notional, 25)
        self.assertEqual(o.legs[0]['qty'], o.legs[1]['qty'])
        self.assertLess(o.net_profit, o.notional*o.gross_edge)

    def test_partial_buy_depth_rejected(self):
        self.store.update('coinbase','BTC/USDT',[(99,20)],[(100,.001)])
        self.assertFalse(self.scanner.scan())

    def test_partial_sell_depth_rejected(self):
        self.store.update('kraken','BTC/USDT',[(105,.001)],[(106,20)])
        self.assertFalse(self.scanner.scan())

    def test_exchange_minimum_rejected(self):
        self.store.markets['kraken']['BTC/USDT']['limits']['cost']['min'] = 50
        self.assertFalse(self.scanner.scan())

    def test_stale_exchange_timestamp_rejected(self):
        self.store.get('coinbase','BTC/USDT').exchange_ts = time.time()-10
        self.assertFalse(self.scanner.scan())

    def test_missing_metadata_rejected(self):
        self.store.markets.clear()
        self.assertFalse(self.scanner.scan())

    def test_unsynchronized_books_rejected(self):
        self.store.get('coinbase','BTC/USDT').ts -= 1
        self.assertFalse(self.scanner.scan())

    def test_invalid_books_rejected(self):
        for bids, asks in [([(101,1)],[(100,1)]), ([(float('nan'),1)],[(100,1)]), ([(99,-1)],[(100,1)])]:
            with self.subTest(bids=bids):
                self.store.update('coinbase','BTC/USDT',bids,asks)
                self.assertFalse(self.scanner.scan())

    def test_future_book_rejected(self):
        self.store.get('coinbase','BTC/USDT').ts += 10
        self.assertFalse(self.scanner.scan())

    def test_quote_basis_cannot_mix(self):
        self.cfg['arb_groups']['BTC']['kraken'] = 'BTC/USD'
        self.assertFalse(self.scanner.scan())

    def test_paper_never_creates_unfunded_inventory(self):
        ex = self.paper()
        self.assertIsNone(ex.execute(self.opportunity(), {'BTC':100}))
        self.assertEqual(ex.balances['kraken']['USDT'],500)
        ex.seed_inventory(self.store)
        self.assertTrue(all(q >= 0 for b in ex.balances.values() for q in b.values()))
        self.assertLess(ex.realized_pnl,0)
        o = self.opportunity()
        before = sum(b['USDT'] for b in ex.balances.values())
        fill = ex.execute(o, {'BTC':100})
        self.assertIsNotNone(fill)
        self.assertAlmostEqual(sum(b['USDT'] for b in ex.balances.values())-before, o.net_profit)

    def test_restart_preserves_balances_and_daily_costs(self):
        ex = self.paper()
        ex.seed_inventory(self.store)
        ex.execute(self.opportunity(), {'BTC':100})
        restored = self.paper()
        self.assertEqual(restored.balances,ex.balances)
        self.assertEqual(restored.pnl_today(),ex.pnl_today())
        self.assertEqual(restored.realized_pnl,ex.realized_pnl)

    def test_changed_economics_refuses_old_evidence(self):
        self.paper()
        self.cfg['venues']['coinbase']['taker_fee'] *= .5
        with self.assertRaisesRegex(ValueError, 'configuration changed'):
            PaperExecutor(self.cfg)

    def test_projected_inventory_limit(self):
        ex = self.paper()
        ex.seed_inventory(self.store)
        self.cfg['risk']['max_open_skew_usdt'] = 1
        before = copy.deepcopy(ex.balances)
        self.assertIsNone(ex.execute(self.opportunity(), {'BTC':100}))
        self.assertEqual(ex.balances,before)

    def test_confirmation_requires_new_books_on_both_legs(self):
        ex = self.paper()
        evidence = Evidence(self.cfg,ex.db)
        now = time.time()
        self.assertFalse(evidence.update(self.scanner.scan(), True, now))
        self.assertFalse(evidence.update(self.scanner.scan(), True, now+1))
        self.books()
        ready = evidence.update(self.scanner.scan(), True, now+1.2)
        self.assertEqual(len(ready),1)
        evidence.result(ready[0][0], 'paper_filled', pnl=ready[0][1].net_profit)
        self.books()
        self.assertFalse(evidence.update(self.scanner.scan(), True, now+2))

    def test_restart_does_not_reuse_continuous_window(self):
        ex=self.paper()
        evidence=Evidence(self.cfg,ex.db)
        now=time.time()
        evidence.update(self.scanner.scan(),True,now)
        self.books()
        ready=evidence.update(self.scanner.scan(),True,now+1)
        evidence.result(ready[0][0],'paper_filled',pnl=ready[0][1].net_profit)
        restarted=Evidence(self.cfg,ex.db)
        self.books()
        self.assertFalse(restarted.update(self.scanner.scan(),True,now+2))
        self.assertEqual(ex.db.execute('SELECT count(*) FROM windows').fetchone()[0],1)

    def test_new_window_after_observed_gap(self):
        evidence=Evidence(self.cfg,self.paper().db)
        now=time.time()
        evidence.update(self.scanner.scan(),True,now)
        evidence.update([],False,now+4)
        self.books()
        evidence.update(self.scanner.scan(),True,now+5)
        self.books()
        self.assertEqual(len(evidence.update(self.scanner.scan(),True,now+6)),1)

    def test_disappearing_edge_expires(self):
        evidence = Evidence(self.cfg,self.paper().db)
        now=time.time()
        evidence.update(self.scanner.scan(),True,now)
        evidence.update([],False,now+.2)
        self.books()
        self.assertFalse(evidence.update(self.scanner.scan(),True,now+1))

    def test_offline_time_not_counted(self):
        evidence = Evidence(self.cfg,self.paper().db)
        evidence.update([],True,0)
        evidence.update([],True,86400)
        self.assertEqual(evidence.funding()['observation_hours'],0)

    def test_funding_gate_no_synthetic_readiness(self):
        evidence=Evidence(self.cfg,self.paper().db)
        result=evidence.funding()
        self.assertFalse(result['ready'])
        self.assertTrue(any('verify account' in b for b in result['blockers']))
        self.assertIsNone(result['proposal'])

    def test_funding_proposal_only_after_all_requirements(self):
        ex=self.paper()
        evidence=Evidence(self.cfg,ex.db)
        now=time.time()
        for v in self.cfg['venues']:
            ex.db.execute('INSERT INTO fee_verifications VALUES(?,?,?)', (v,now,json.dumps({'BTC/USDT':self.cfg['venues'][v]['taker_fee']})))
        ex.mark_equity({'BTC':100})
        self.cfg['funding'].update(min_observation_hours=1,min_paper_fills=1,min_days_with_fills=1,min_net_pnl_usdt=.1)
        ex.db.execute('UPDATE observation_time SET seconds=3600')
        ex.db.execute('INSERT INTO windows VALUES(1,?,?,?,?,?,?,?)', ('route',now,now,'paper_filled','',json.dumps(self.opportunity().__dict__),1))
        ex.db.commit()
        report=evidence.funding(now=now)
        self.assertTrue(report['ready'])
        self.assertIn('coinbase',report['proposal']['balances'])
        self.assertIn('BTC',report['proposal']['balances']['kraken'])
        self.assertFalse(evidence.funding('kill switch',now=now)['ready'])

    def test_kill_switch_without_opportunities(self):
        risk=RiskManager(self.cfg)
        Path(self.cfg['risk']['kill_switch_file']).touch()
        self.assertTrue(risk.check_halt(0))
        self.assertFalse(risk.allow(self.opportunity(),0,0))

    def test_nan_configuration_rejected(self):
        self.cfg['scanner']['min_net_edge']=float('nan')
        with self.assertRaises(ValueError): validate(self.cfg)

    def test_process_lock_excludes_second_engine_and_releases(self):
        path=Path(self.tmp.name)/'engine.lock'
        with ProcessLock(path):
            with self.assertRaises(RuntimeError):
                with ProcessLock(path): pass
        with ProcessLock(path): pass

    def test_inventory_loss_counts_toward_daily_limit(self):
        ex=self.paper()
        ex.seed_inventory(self.store)
        ex.mark_equity({'BTC':100})
        ex.mark_equity({'BTC':90})
        self.assertLess(ex.risk_pnl_today(),-10)

    def test_triangular_full_legs_settle_and_short_depth_rejected(self):
        self.cfg['scanner']['triangular_enabled']=True
        self.cfg['triangles']={'kraken':[['BTC/USDT','ETH/BTC','ETH/USDT']]}
        for symbol in ('ETH/BTC','ETH/USDT'):
            market=copy.deepcopy(self.store.markets['kraken']['BTC/USDT'])
            market.update(symbol=symbol,id=symbol)
            market['limits']['cost']['min']=.00001
            self.store.markets['kraken'][symbol]=market
        self.store.update('kraken','BTC/USDT',[(99,20)],[(100,20)])
        self.store.update('kraken','ETH/BTC',[(.0099,200)],[(.01,200)])
        self.store.update('kraken','ETH/USDT',[(1.1,200)],[(1.11,200)])
        scanner=TriangularScanner(self.cfg,self.store)
        opps=scanner.scan()
        self.assertTrue(opps)
        ex=self.paper()
        o=opps[0]
        self.assertEqual(len(o.legs),3)
        self.assertIsNotNone(ex.execute(o,{'BTC':100,'ETH':1.1}))
        self.store.update('kraken','ETH/BTC',[(.0099,200)],[(.01,.001)])
        self.assertFalse(scanner.scan())


class LiveTests(BaseFixture):
    def live(self, amounts=(.24,.24), fail=False):
        ex=LiveExecutor(self.cfg)
        self.addCleanup(ex.close)
        ex.balances={'coinbase':{'USDT':500,'BTC':1},'kraken':{'USDT':500,'BTC':1}}
        ex.free_balances=copy.deepcopy(ex.balances)
        ex.baseline={'coinbase':{'BTC':1},'kraken':{'BTC':1}}
        ex.refresh_balances=AsyncMock()
        for index,venue in enumerate(('coinbase','kraken')):
            adapter=self.store.exchanges[venue]
            adapter.create_order=AsyncMock(side_effect=TimeoutError() if fail and index==0 else None,
                                           return_value={'id':venue+'123'})
            adapter.fetch_order=AsyncMock(return_value={'id':venue+'123','status':'closed','filled':amounts[index],
                'cost':amounts[index]*(100 if index==0 else 105),'fees':[{'currency':'USDT','cost':.01}]})
            ex.exchanges[venue]=adapter
        return ex

    def test_live_confirmed_pair_and_journal(self):
        ex=self.live()
        result=asyncio.run(ex.execute_live(self.opportunity(),self.store,{'BTC':100}))
        self.assertIsNotNone(result, ex.last_rejection or ex.halted_reason)
        self.assertEqual(ex.db.execute("SELECT count(*) FROM live_orders WHERE status='settled'").fetchone()[0],2)
        for adapter in ex.exchanges.values():
            self.assertEqual(adapter.create_order.await_args.args[-1]['timeInForce'],'IOC')
            self.assertEqual(adapter.create_order.await_count,1)

    def test_live_unknown_submission_halts_and_never_retries(self):
        ex=self.live(fail=True)
        self.assertIsNone(asyncio.run(ex.execute_live(self.opportunity(),self.store,{'BTC':100})))
        self.assertIsNotNone(ex.halted_reason)
        self.assertIsNone(asyncio.run(ex.execute_live(self.opportunity(),self.store,{'BTC':100})))
        self.assertEqual(ex.exchanges['coinbase'].create_order.await_count,1)
        restored=LiveExecutor(self.cfg)
        self.addCleanup(restored.close)
        self.assertIsNotNone(restored.halted_reason)

    def test_live_partial_mismatch_persists_halt(self):
        ex=self.live(amounts=(.24,.12))
        self.assertIsNone(asyncio.run(ex.execute_live(self.opportunity(),self.store,{'BTC':100})))
        self.assertIn('Unmatched',ex.halted_reason)

    def test_live_insufficient_free_cash_no_submission(self):
        ex=self.live()
        ex.free_balances['coinbase']['USDT']=0
        self.assertIsNone(asyncio.run(ex.execute_live(self.opportunity(),self.store,{'BTC':100})))
        self.assertEqual(ex.exchanges['coinbase'].create_order.await_count,0)

    def test_live_stale_opportunity_no_submission(self):
        ex=self.live()
        o=self.opportunity(); o.ts-=10
        self.assertIsNone(asyncio.run(ex.execute_live(o,self.store,{'BTC':100})))
        self.assertEqual(ex.exchanges['coinbase'].create_order.await_count,0)

    def test_live_missing_actual_fees_halts(self):
        ex=self.live()
        ex.exchanges['coinbase'].fetch_order.return_value['fees']=[]
        self.assertIsNone(asyncio.run(ex.execute_live(self.opportunity(),self.store,{'BTC':100})))
        self.assertIsNotNone(ex.halted_reason)

    def test_live_both_unfilled_no_profit(self):
        ex=self.live(amounts=(0,0))
        self.assertIsNone(asyncio.run(ex.execute_live(self.opportunity(),self.store,{'BTC':100})))
        self.assertIsNone(ex.halted_reason)
        self.assertEqual(len(ex.fills),0)

    def test_kill_arrives_during_balance_refresh_no_orders(self):
        ex=self.live()
        async def refresh():
            Path(self.cfg['risk']['kill_switch_file']).touch()
        ex.refresh_balances=refresh
        self.assertIsNone(asyncio.run(ex.execute_live(self.opportunity(),self.store,{'BTC':100})))
        self.assertEqual(ex.exchanges['coinbase'].create_order.await_count,0)

    def test_private_failure_during_balance_refresh_no_orders(self):
        ex=self.live()
        async def refresh():
            ex.halt('Private order stream failed during preflight')
        ex.refresh_balances=refresh
        self.assertIsNone(asyncio.run(ex.execute_live(self.opportunity(),self.store,{'BTC':100})))
        for venue in ex.exchanges.values():venue.create_order.assert_not_called()
        self.assertEqual(ex.db.execute('SELECT count(*) FROM live_orders').fetchone()[0],0)

    def test_invalid_overfill_response_halts(self):
        ex=self.live(amounts=(10,10))
        self.assertIsNone(asyncio.run(ex.execute_live(self.opportunity(),self.store,{'BTC':100})))
        self.assertIsNotNone(ex.halted_reason)

    def test_account_fee_increase_blocks(self):
        ex=self.live()
        ex.exchanges['coinbase'].fetch_trading_fees=AsyncMock(return_value={'BTC/USDT':{'taker':.02}})
        with self.assertRaisesRegex(ValueError,'actual taker fee'):
            asyncio.run(ex.verify_fees())

    def test_missing_acknowledgement_id_halts(self):
        ex=self.live()
        ex.exchanges['coinbase'].create_order.return_value={}
        self.assertIsNone(asyncio.run(ex.execute_live(self.opportunity(),self.store,{'BTC':100})))
        self.assertIsNotNone(ex.halted_reason)


if __name__=='__main__': unittest.main()
