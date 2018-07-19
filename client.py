import datetime
import logging
import sqlite3
import threading
from typing import Any, Dict, List
from collections import defaultdict

import requests

import pandas as pd
from config import configuration

log = logging.getLogger(__name__)


class SmarketsClient:
    def __init__(self):
        self.auth_lock = threading.Lock()
        self.auth_token = None

    def _auth_headers(self):
        return {'Authorization': 'Session-Token ' + self.auth_token}

    def init_session(self):
        log.info('initiating session')
        self.auth_token = configuration['auth'].get('auth_token')
        if not self.auth_token:
            with self.auth_lock:
                response = requests.post(
                    f'{configuration["api"]["base_url"]}sessions/',
                    json={
                        'username': configuration['auth']['login'],
                        'password': configuration['auth']['password'],
                    },
                ).json()
                self.auth_token = response.get('token')
        log.info(f'auth token: {self.auth_token}')

    def reauth_session(self):
        log.info('renewing session')
        with self.auth_lock:
            response = requests.post(f'{configuration["api"]["base_url"]}sessions/reauth/').json()
            self.auth_token = response.get('token')
        log.info(f'new auth token: {self.auth_token}')

    def place_bet(
        self,
        market_id,
        contract_id,
        price,
        quantity,
        side,
    ):
        log.info(f'placing order: m_id {market_id}: c_id {contract_id} \t {side} {quantity} @ {price}')
        with self.auth_lock:
            response = requests.post(
                f'{configuration["api"]["base_url"]}orders/',
                json={
                    'market_id': market_id,
                    'contract_id': contract_id,
                    'price': price,
                    'quantity': quantity,
                    'reference_id': str(int(datetime.datetime.utcnow().timestamp())),
                    'side': side,
                },
                headers=self._auth_headers(),
            ).json()
        log.info(
            f'''order placed: m_id {market_id}: c_id {contract_id} \t {side} {quantity} @ {price}|'''
            f'''balance:{response["available_balance"]} executed:{response["total_executed_quantity"]}'''
            f''' exposure:{response["exposure"]}'''
        )

    def cancel_bet(self, order_id):
        with self.auth_lock:
            requests.delete(
                f'{configuration["api"]["base_url"]}orders/{order_id}/',
                headers=self._auth_headers(),
            )

    def get_orders(self, states):
        orders = []
        states_to_fetch = '&'.join([f'states={state}' for state in states])
        next_page = f'?limit={configuration["api"]["chunk_size"]}&{states_to_fetch}'
        with self.auth_lock:
            while next_page:
                response = requests.get(
                    f'{configuration["api"]["base_url"]}orders/{next_page}',
                    headers=self._auth_headers(),
                ).json()
                log.info(response)
                orders += response['orders']
                next_page = response['pagination']['next_page']
        return orders

    def get_available_events(
        self,
        states: List[str],
        types: List[str],
        start_datetime_max: datetime.datetime,
        limit: int,
    ):
        states_filter = '&'.join([f'states={state}' for state in states])
        types_filter = '&'.join([f'types={type_}' for type_ in types])
        page_filter = (
            f'?{states_filter}&{types_filter}&sort=id&'
            f'limit={limit}&start_datetime_max={start_datetime_max}'
        )
        events = []
        while page_filter:
            request_url = f'{configuration["api"]["base_url"]}events/{page_filter}'
            current_page = self._client_wrapper(request_url)
            events += current_page['events']
            page_filter = current_page['pagination']['next_page']

        return events

    def get_markets(self, market_ids: List[str], with_volumes: bool=False):
        markets = ','.join(market_ids)
        request_url = f'{configuration["api"]["base_url"]}markets/{markets}/?with_volumes={with_volumes}'
        return self._client_wrapper(request_url).get('markets')

    def get_related_markets(self, events):
        markets = []
        event_ids = [event['id'] for event in events]
        i = 0
        chunk_size = configuration["api"]["chunk_size"]
        while i * chunk_size < len(event_ids):
            events_to_fetch = ','.join(
                event_ids[i * chunk_size:(i + 1) * chunk_size]
            )
            request_url = (
                f'''{configuration["api"]["base_url"]}events/{events_to_fetch}/markets/'''
                f'''?sort=event_id,display_order&with_volumes=true'''
            )
            markets += self._client_wrapper(request_url)['markets']
            i += 1
        return markets

    def get_related_contracts(self, markets):
        contracts = []
        market_ids = [market['id'] for market in markets]
        i = 0
        chunk_size = configuration["api"]["chunk_size"]
        while i * chunk_size < len(market_ids):
            markets_to_fetch = ','.join(
                market_ids[i * chunk_size:(i + 1) * chunk_size]
            )
            request_url = (
                f'''{configuration["api"]["base_url"]}markets/{markets_to_fetch}/contracts/'''
            )
            contracts += self._client_wrapper(request_url)['contracts']
            i += 1
        return contracts

    def get_quotes(self, market_ids: List[str]):
        quotes = []
        i = 0
        chunk_size = configuration["api"]["chunk_size"]
        while i * chunk_size < len(market_ids):
            markets_to_fetch = ','.join(
                market_ids[i * chunk_size:(i + 1) * chunk_size]
            )
            request_url = f'{configuration["api"]["base_url"]}markets/{markets_to_fetch}/quotes/'
            quotes += [self._client_wrapper(request_url)]
            i += 1
        quotes_result = {}
        for quote_entry in quotes:
            for contract_id, order_book in quote_entry.items():
                quotes_result[contract_id] = order_book
        return quotes_result

    def get_accounts(self):
        response = None
        with self.auth_lock:
            response = requests.get(
                f'{configuration["api"]["base_url"]}accounts/', headers=self._auth_headers()
            ).json()
        return response

    def get_account_activity(self, market_id: str=None):
        market_filter = '' if not market_id else f'&market_id={market_id}'
        next_page = f'?limit={configuration["api"]["chunk_size"]}{market_filter}'
        activity = []
        while next_page:
            with self.auth_lock:
                response = requests.get(
                    f'{configuration["api"]["base_url"]}accounts/activity/{next_page}',
                    headers=self._auth_headers(),
                ).json()
            activity += response['account_activity']
            next_page = response['pagination']['next_page']
        return activity

    def _client_wrapper(self, url: str) -> Dict[str, Any]:
        log.info(f'calling url: {url}')
        return requests.get(url).json()


class BacktestClient(SmarketsClient):
    def __init__(self, time_interval):
        self.orders = defaultdict(list)
        self.market_price_iters = {}
        self.time_interval = time_interval

    def place_bet(
        self, market_id, contract_id, price, quantity, side,
    ):
        self.orders[contract_id].append({
            'market_id': market_id,
            'contract_id': contract_id,
            'price': price,
            'quantity': quantity,
            'side': side,
        })

    def _get_quotes_iter(self, market_id):
        contracts = self.get_related_contracts([{'id': market_id}])
        contract_ids = ','.join(str(contract['id']) for contract in contracts)
        with sqlite3.connect('tickerplant.db') as con:
            df = pd.read_sql(
                f'SELECT * FROM ticks WHERE contract_id IN ({contract_ids})',
                con, parse_dates=['timestamp'], index_col=['id'],
            )
            if df.empty:
                log.error(f'Market id {market_id} does not have any contract on tickerplant')
                return ()

        # turn the tick table into another table wher the index are the timestamps
        # and the columns are the books of each contract
        dfiter = (df
            .set_index(['contract_id', 'timestamp'])
            .unstack('contract_id')
            .resample(self.time_interval)
            .ffill()
            .iterrows()
        )

        def row_to_book(row):
            quotes = {}
            for contract_id, book in row.unstack('contract_id').T.iterrows():
                bids = [
                    {'price': book.bp1, 'quantity': book.bq1},
                    {'price': book.bp2, 'quantity': book.bq2},
                    {'price': book.bp3, 'quantity': book.bq3},
                ]
                offers = [
                    {'price': book.op1, 'quantity': book.oq1},
                    {'price': book.op2, 'quantity': book.oq2},
                    {'price': book.op3, 'quantity': book.oq3},
                ]
                quotes[contract_id] = {
                    'bids': [bid for bid in bids if not pd.np.isnan(bid['price'])],
                    'offers': [offer for offer in offers if not pd.np.isnan(offer['price'])],
                }
            return quotes

        return (row_to_book(row) for _, row in dfiter)

    def collect_iters(self, market_ids):
        iters = []
        for market_id in market_ids:
            if market_id not in self.market_price_iters:
                self.market_price_iters[market_id] = self._get_quotes_iter(market_id)
            iters.append(self.market_price_iters[market_id])
        return iters

    def get_quotes(self, market_ids: List[str]):
        iters = self.collect_iters(market_ids)
        quotes = {}
        for iter in iters:
            quotes.update(next(iter, {}))
        return quotes

    def get_account_activity(self, market_id):
        pass
