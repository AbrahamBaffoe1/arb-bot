import copy
import json
import unittest
from decimal import Decimal
from unittest.mock import Mock
from src.kraken_feed import CheckedKraken,checksum


class KrakenChecksumTests(unittest.TestCase):
    # Kraken's published v2 checksum example, expected CRC32 3310070434.
    # https://docs.kraken.com/exchange/guides/websockets/book-checksum-v2
    def levels(self):
        bids=[('45283.5','0.10000000'),('45283.4','1.54582015'),('45282.1','0.10000000'),
              ('45281.0','0.10000000'),('45280.3','1.54592586'),('45279.0','0.07990000'),
              ('45277.6','0.03310103'),('45277.5','0.30000000'),('45277.3','1.54602737'),('45276.6','0.15445238')]
        asks=[('45285.2','0.00100000'),('45286.4','1.54571953'),('45286.6','1.54571109'),
              ('45289.6','1.54560911'),('45290.2','0.15890660'),('45291.8','1.54553491'),
              ('45294.7','0.04454749'),('45296.1','0.35380000'),('45297.5','0.09945542'),('45299.5','0.18772827')]
        return {side:[[Decimal(p),Decimal(q)] for p,q in rows] for side,rows in [('bids',bids),('asks',asks)]}

    def message(self):
        levels=self.levels()
        return dict(channel='book',type='snapshot',data=[dict(symbol='BTC/USD',checksum=3310070434,
            **{side:[dict(price=p,qty=q) for p,q in rows] for side,rows in levels.items()})])

    def test_matches_exchange_reference_checksum(self):
        self.assertEqual(checksum(self.levels()),3310070434)

    def test_snapshot_verified_before_delivery(self):
        ex=CheckedKraken();client=Mock();client.subscriptions={}
        ex.handle_order_book(client,self.message())
        client.resolve.assert_called_once()
        client.reject.assert_not_called()

    def test_corrupt_update_invalidates_book(self):
        ex=CheckedKraken();client=Mock();client.subscriptions={}
        ex.handle_order_book(client,self.message())
        client.resolve.reset_mock()
        ex.handle_order_book(client,dict(type='update',data=[dict(symbol='BTC/USD',bids=[dict(price=Decimal('45283.5'),qty=Decimal('0.2'))],checksum=1)]))
        self.assertNotIn('BTC/USD',ex.orderbooks)
        client.resolve.assert_not_called()
        client.reject.assert_called_once()

    def test_short_book_and_deletion(self):
        ex=CheckedKraken();client=Mock();client.subscriptions={}
        levels={'bids':[[Decimal('100.0'),Decimal('1.000')]],'asks':[[Decimal('101.0'),Decimal('2.000')]]}
        data=dict(symbol='BTC/USD',checksum=checksum(levels),**{s:[dict(price=p,qty=q) for p,q in rows] for s,rows in levels.items()})
        ex.handle_order_book(client,dict(type='snapshot',data=[data]))
        levels['bids']=[]
        ex.handle_order_book(client,dict(type='update',data=[dict(symbol='BTC/USD',checksum=checksum(levels),bids=[dict(price=Decimal('100.0'),qty=0)])]))
        self.assertEqual(len(ex.orderbooks['BTC/USD']['bids']),0)
        self.assertEqual(client.resolve.call_count,2)
        client.reject.assert_not_called()
