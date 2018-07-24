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

        for contract in live_contracts:
            with self.connection:
                prices = list(self.connection.execute(
                    'SELECT bp1, op1 FROM ticks WHERE contract_id = {} AND bp1 is not null AND op1 is not null  ORDER BY timestamp DESC LIMIT 11'.format(contract['id'])
                ))
            if len(prices) == 0:
                log.warning('No quotes for the contract: %s', contract)
                continue
            else:
                current_price, past_prices = prices[0], prices[1:]
                bprice_mean = mean([price[0] for price in past_prices])
                oprice_mean = mean([price[1] for price in past_prices])
                if current_price[0] > bprice_mean:
                    log.info('BUY', current_price[0], bprice_mean)
                    self.smk_client.place_order(
                        contract['market_id'],
                        contract['id'],
                        current_price,
                        FIXED_QUANTITY,
                        'buy',
                    )
                elif current_price[1] < oprice_mean:
                    log.info('SELL', current_price[1], oprice_mean)
                    self.smk_client.place_order(
                        contract['market_id'],
                        contract['id'],
                        current_price,
                        FIXED_QUANTITY,
                        'sell',
                    )


    def run(self):
        # 5. TODO fetch the contracts
        # contracts = ...
        while True:
            loop_start = datetime.datetime.utcnow()
            # 6. TODO fetch the positions
            # positions = ...

            self.strategy(contracts, positions)

            elapsed = datetime.datetime.utcnow() - loop_start
            time.sleep(configuration['misc']['bot_tick_interval'] - elapsed.seconds)
