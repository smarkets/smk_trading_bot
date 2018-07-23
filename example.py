import datetime
import logging
import sqlite3
import threading
import time
from logging.config import fileConfig
from pprint import pprint

import client
from config import configuration

fileConfig('logging.config', disable_existing_loggers=False)

log = logging.getLogger(__name__)

class ExampleBot(threading.Thread):
    def __init__(self, smk_client, markets):
        self.connection = sqlite3.connect(configuration["misc"]["ticker_plant_path"], check_same_thread=False)
        self.smk_client = smk_client
        self.markets = markets
        super().__init__()

    def strategy(self, live_contracts, positions):
        def mean(l):
            return float(sum(l)) / len(l)

        for position in positions:
            print(position)

        for contract in live_contracts:
            with self.connection:
                prices = self.connection.execute(
                    'SELECT bp1, op1 FROM ticks WHERE contract_id = {} AND bp1 is not null AND op1 is not null  ORDER BY timestamp DESC LIMIT 11'.format(contract['id'])
                )
            prices = list(prices)
            if len(prices) == 0:
                log.warning('No quotes for the contract: %s', contract)
                continue
            else:
                ma = prices[1:]
                current_price = prices[0]
                print(ma, current_price)
                if current_price[0] > mean([price[0] for price in ma]):
                    print('BUY', current_price[0],mean([price[0] for price in ma]) )
                elif current_price[1] < mean([price[1] for price in ma]):
                    print('SELL', current_price[1],mean([price[1] for price in ma]) )

    def run(self):
        #1. TODO fetch the markets and contracts
        contracts = self.smk_client.get_related_contracts(self.markets)
        while True:
            loop_start = datetime.datetime.utcnow()
            ###
            # 2. TODO fetch the positions
            positions = self.smk_client.get_orders(states=['created', 'filled', 'partial'])

            self.strategy(contracts, positions)

            elapsed = datetime.datetime.utcnow() - loop_start
            time.sleep(configuration['misc']['bot_tick_interval'] - elapsed.seconds)
