"""Precision-preserving Kraken v2 book checks for the pinned CCXT adapter.

Kraken requires the original decimal digits in its CRC32 payload. Python float
decoding loses trailing zeros before CCXT's built-in checksum sees the levels.
https://docs.kraken.com/exchange/guides/websockets/book-checksum-v2
"""
import json
import zlib
from decimal import Decimal
import ccxt.pro as ccxt
from ccxt.base.errors import ChecksumError


def checksum(levels):
    payload=''.join(format(value,'f').replace('.','').lstrip('0')
                    for side in ('asks','bids') for level in list(levels[side])[:10] for value in level[:2])
    return zlib.crc32(payload.encode()) & 0xffffffff


class CheckedKraken(ccxt.kraken):
    def client(self,url):
        client=super().client(url)
        if not getattr(client,'decimal_books',False):
            def decode(data):
                message=json.loads(data,parse_float=Decimal)
                client.on_message_callback(client,message)
            client.handle_text_or_binary_message=decode
            client.decimal_books=True
        return client

    def handle_order_book(self,client,message):
        for data in message.get('data',[]):
            symbol=data['symbol']
            key=self.get_message_hash('orderbook',None,symbol)
            try:
                if message['type']=='snapshot':
                    # watch_order_book registers the requested depth in options.
                    depth=self.options.get('watchOrderBook',{}).get('limit',25)
                    self.orderbooks[symbol]=self.order_book({},depth)
                if symbol not in self.orderbooks:
                    raise ValueError('Book update arrived before snapshot')
                book=self.orderbooks[symbol]
                for side in ('asks','bids'):
                    for delta in data.get(side,[]):
                        price,qty=Decimal(str(delta['price'])),Decimal(str(delta['qty']))
                        if not price.is_finite() or not qty.is_finite() or price<=0 or qty<0:
                            raise ValueError('Invalid book level')
                        book[side].store(price,qty)
                book.limit()
                if checksum(book)!=data.get('checksum'):
                    raise ValueError('CRC32 mismatch')
                book['symbol']=symbol
                book['datetime']=data.get('timestamp')
                book['timestamp']=self.parse8601(book['datetime'])
                client.resolve(book,key)
            except (ValueError,KeyError,ArithmeticError) as exc:
                self.orderbooks.pop(symbol,None)
                client.subscriptions.pop(key,None)
                client.reject(ChecksumError(f'kraken {symbol}: {exc}'),key)
