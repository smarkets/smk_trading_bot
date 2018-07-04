import datetime
import logging
import threading
from collections import defaultdict
from itertools import groupby
from logging.config import fileConfig

import requests

import utils
import client

# fileConfig('logging.config')

log = logging.getLogger(__name__)

class DumbBot(threading.Thread):
    def __init__(self, smk_client):
        self.smk_client = smk_client

    def calculate_ev(self):
        orders = self.smk_client.get_orders(states=['filled', 'partial'])
        unique_markets = set([order['market_id'] for order in orders])
        unique_markets_quotes = self.smk_client.get_quotes(list(unique_markets))
        log.info(unique_markets_quotes)
        contract_book = defaultdict(list)

        for contract_id, order in groupby(orders, lambda x: x['contract_id']):
            self._calculate_contract_ev(list(orders), unique_markets_quotes.get(contract_id))
            # contract_book[order['contract_id']].append({
                # 'price': (10000.0 / order['average_price_matched']),
                # 'stake': (order['quantity_filled'] / (10000.0 / order['average_price_matched'])),
                # 'ev': (order['quantity_filled'] / (10000.0 / order['average_price_matched'])) * 
            # })
        log.info(contract_book)
    def _calculate_contract_ev(self, contract_orders, quotes):
        order_prices = [
            {
                'price': 10000.0 / order['average_price_matched'],
                'stake': order['quantity_filled'] / (10000.0 / order['average_price_matched']),
                'side': order['side'],
            }
            for order in contract_orders
        ]
        quotes = {
            'sell': 10000.0 / max(tick['price'] for tick in quotes['bids']),
            'buy': 10000.0 / min(tick['price'] for tick in quotes['offers']),
        }
        log.info(order_prices)
        log.info(quotes)
        log.info([
            (100.0 / (quotes['sell'] if o['side']=='buy' else quotes['buy']) - 100.0/ o['price']) * o['stake']
            for o
            in order_prices
        ])

    def run(self):
        pass


client = client.SmarketsClient()

client.init_session()
# client.place_bet(
    # "6672405",
    # "21490523",
    # 50,
    # 50000,
    # 'buy',
# )

# client.cancel_bet('202547466272702478')

log.info(client.get_orders(states=['created', 'filled', 'partial']))
log.info(DumbBot(client).calculate_ev())
