import logging
import datetime
import sqlite3
import threading
import time
from logging.config import fileConfig
from typing import Any, Dict, List

import requests

from config import configuration

log = logging.getLogger(__name__)

class QuoteFetcher(threading.Thread):
    def __init__(self, smk_client):
        self.smk_client = smk_client
        super().__init__()

    def _initialize_ticker_plant(self):
        connection = sqlite3.connect(configuration["misc"]["ticker_plant_path"])
        with connection:
            connection.execute("""
                CREATE TABLE IF NOT EXISTS ticks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    contract_id INTEGER,
                    timestamp DATETIME,
                    bp1 INTEGER,
                    bq1 INTEGER,
                    bp2 INTEGER,
                    bq2 INTEGER,
                    bp3 INTEGER,
                    bq3 INTEGER,
                    op1 INTEGER,
                    oq1 INTEGER,
                    op2 INTEGER,
                    oq2 INTEGER,
                    op3 INTEGER,
                    oq3 INTEGER
                )
            """)

    def _store_ticks(self, quotes):
        transformed_quotes = []
        now = datetime.datetime.utcnow()

        def value_from_order_book(order_book, order_type, depth, value_type):
            return (
                order_book[order_type][depth][value_type]
                if order_type in order_book and len(order_book[order_type]) > depth
                else None
            )

        def flatten_order_book(order_book):
            for order_type in ['bids', 'offers']:
                for depth in range(3):
                    for value_type in ['price', 'quantity']:
                        yield value_from_order_book(order_book, order_type, depth, value_type)

        for contract_id, order_book in quotes.items():
            transformed_quotes.append(
                tuple([
                    contract_id,
                    now,
                    *[order_book_entry for order_book_entry in flatten_order_book(order_book)],
                ]),
            )

        connection = sqlite3.connect(configuration["misc"]["ticker_plant_path"])
        with connection:
            connection.executemany(
                """
                    INSERT INTO ticks(contract_id, timestamp, bp1, bq1, bp2, bq2,bp3, bq3, op1, oq1, op2, oq2, op3, oq3)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                transformed_quotes,
            )


    def run(self):
        self._initialize_ticker_plant()
        events = self.smk_client.get_available_events(
            ['upcoming', 'live'],
            ['football_match'],
            datetime.datetime.utcnow(),
            20,
        )
        log.info(f'collected {len(events)} events matching filter')
        markets = self.smk_client.get_related_markets(events)
        log.info(f'collected {len(markets)} markets')

        while True:
            quotes = self.smk_client.get_quotes([market['id'] for market in markets])
            self._store_ticks(quotes)
            log.info(f'collected the ticks, now sleeping for {configuration["misc"]["sleep_interval"]} seconds')
            time.sleep(configuration["misc"]["sleep_interval"])


class Authenticator(threading.Thread):
    def __init__(self, smk_client):
        self.smk_client = smk_client
        super().__init__()

    def run(self):
        self.smk_client.init_session()
        while True:
            time.sleep(configuration["misc"]["reauth_sleep_interval"])
            self.smk_client.reauth_session()
