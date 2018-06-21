from typing import Any, Dict, List
import datetime
import sqlite3
import time

import requests

BASE_URL = 'https://api.smarkets.com/v3/'
CHUNK_SIZE = 10
TICKER_PLANT_PATH = 'tickerplant.db'
SLEEP_INTERVAL = 10

class TradingBot:
    def _initialize_ticker_plant(self):
        connection = sqlite3.connect(TICKER_PLANT_PATH)
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
            return order_book[order_type][depth][value_type] if len(order_book[order_type]) > depth else None

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

        connection = sqlite3.connect(TICKER_PLANT_PATH)
        with connection:
            connection.executemany(
                """
                    INSERT INTO ticks(contract_id, timestamp, bp1, bq1, bp2, bq2,bp3, bq3, op1, oq1, op2, oq2, op3, oq3)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                transformed_quotes,
            )
    def _client_wrapper(self, url: str) -> Dict[str, Any]:
        print(f'calling url: {url}')
        return requests.get(url).json()

    def _fetch_available_events(
        self,
        states: List[str],
        types: List[str],
        start_datetime_max: datetime.datetime,
        limit: int,
    ):
        states_filter = '&'.join([f'states={state}' for state in states])
        types_filter = '&'.join([f'types={type_}' for type_ in types])
        page_filter = f'?{states_filter}&{types_filter}&sort=id&limit={limit}'
        events = []
        while page_filter:
            request_url = f'{BASE_URL}events/{page_filter}'
            current_page = self._client_wrapper(request_url)
            events += current_page['events']
            page_filter = current_page['pagination']['next_page']

        return events

    def _fetch_related_markets(self, events):
        markets = []
        event_ids = [event['id'] for event in events]
        i = 0
        while i*CHUNK_SIZE < len(event_ids):
            events_to_fetch = ','.join(event_ids[i*CHUNK_SIZE:(i+1)*CHUNK_SIZE])
            request_url = f'{BASE_URL}events/{events_to_fetch}/markets/?sort=event_id,display_order&limit_by_event=1&with_volumes=true'
            markets += self._client_wrapper(request_url)['markets']
            i += 1
        return markets

    def _fetch_positions(self):
        pass

    def _fetch_quotes(self, markets):
        quotes = []
        market_ids = [market['id'] for market in markets]
        i = 0
        while i*CHUNK_SIZE < len(market_ids):
            markets_to_fetch = ','.join(market_ids[i*CHUNK_SIZE:(i+1)*CHUNK_SIZE])
            request_url = f'{BASE_URL}markets/{markets_to_fetch}/quotes/'
            quotes += [self._client_wrapper(request_url)]
            i+=1
        quotes_result = {}
        for quote_entry in quotes:
            for contract_id, order_book in quote_entry.items():
                quotes_result[contract_id] = order_book
        return quotes_result

    def run(self):
        self._initialize_ticker_plant()
        self.events = self._fetch_available_events(
            ['upcoming', 'live'],
            ['tennis_match'],
            datetime.datetime.utcnow(),
            20,
        )
        print(f'collected {len(self.events)} events matching filter')
        self.markets = self._fetch_related_markets(self.events)
        print(f'collected {len(self.markets)} markets')
        while True:
            self.quotes = self._fetch_quotes(self.markets)
            self._store_ticks(self.quotes)
            print(f'collected the ticks, now sleeping for {SLEEP_INTERVAL} seconds')
            time.sleep(SLEEP_INTERVAL)


def main():
    TradingBot().run()


if __name__ == '__main__':
    main()
