import logging
import datetime
import sqlite3
import threading
import time
from itertools import count

import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
import pandas as pd
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


class LiveQuotePlotter(threading.Thread):
    def __init__(self, smk_client):
        self.smk_client = smk_client

    def run(self, market_id):
        market = self.smk_client.get_markets([market_id])[0]
        contracts = self.smk_client.get_related_contracts([market])
        contract_ids = ','.join([contract['id'] for contract in contracts])

        fig, ax = plt.subplots()
        connection = sqlite3.connect('tickerplant.db')

        def frames(_):
            with connection:
                df = pd.read_sql(
                    'SELECT * FROM ticks WHERE contract_id IN ({})'.format(contract_ids),
                    connection, parse_dates=['timestamp'],
                )
            xlim = ax.get_xlim()
            ylim = ax.get_ylim()
            ax.clear()
            df = (df
                .set_index(['timestamp', 'contract_id'])
                [['bp1', 'op1']]
                .unstack('contract_id')
            )
            df.groupby(level=1, axis=1).plot(
                ax=ax, marker='.', drawstyle='steps-post', color=['C0', 'C1', 'C2']
            )
            ax.legend([contract['slug'] for contract in contracts])
            if xlim != (0.0, 1.0) and ylim != (0.0, 1.0):
                ax.set_xlim(xlim)
                ax.set_ylim(ylim)
            return ax.get_lines()

        FuncAnimation(
            fig, frames, frames=count(), blit=False,
            interval=configuration['misc']['sleep_interval'] * 1000,
        )
        plt.show()


if __name__ == '__main__':
    import client
    import logging
    logging.basicConfig(format='%(asctime)s %(levelname)s : %(message)s', level=logging.INFO)
    cli = client.SmarketsClient()
    QuoteFetcher(cli).run()
