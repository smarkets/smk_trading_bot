import datetime
import logging
import threading
from logging.config import fileConfig
from typing import Any, Dict, List

import requests

### CONFIG

BASE_URL = 'https://api.smarkets.com/v3/'
LOGIN = ''
PASSWORD = ''

### RUNTIME CONSTANTS
CHUNK_SIZE = 20

# fileConfig('logging.config')
log = logging.getLogger(__name__)

class SmarketsClient:
    def __init__(self):
        self.auth_lock = threading.Lock()
        self.auth_token = None

    def _auth_headers(self):
        return {'Authorization': 'Session-Token '+self.auth_token}

    def init_session(self):
        log.info('initiating session')
        with self.auth_lock:
            response = requests.post(f'{BASE_URL}sessions/', json={'username': LOGIN, 'password': PASSWORD}).json()
            self.auth_token = response.get('token')
        log.info(f'new auth token: {self.auth_token}')

    def reauth_session(self):
        log.info('renewing session')
        with self.auth_lock:
            response = requests.post(f'{BASE_URL}sessions/reauth/').json()
        log.info(f'new auth token: {self.auth_token}')

    def place_bet(
        self,
        market_id,
        contract_id,
        price,
        quantity,
        side,
    ):
        with self.auth_lock:
            response = requests.post(
                f'{BASE_URL}orders/',
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
        log.info(response)

    def cancel_bet(self, order_id):
        with self.auth_lock:
            response = requests.delete(
                f'{BASE_URL}orders/{order_id}/',
                headers=self._auth_headers(),
            )

    def get_orders(self, states):
        orders = []
        states_to_fetch = '&'.join([f'states={state}' for state in states])
        next_page = f'?limit={CHUNK_SIZE}&{states_to_fetch}'
        with self.auth_lock:
            while next_page:
                response = requests.get(
                    f'{BASE_URL}orders/{next_page}',
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
        page_filter = f'?{states_filter}&{types_filter}&sort=id&limit={limit}'
        events = []
        while page_filter:
            request_url = f'{BASE_URL}events/{page_filter}'
            current_page = self._client_wrapper(request_url)
            events += current_page['events']
            page_filter = current_page['pagination']['next_page']

        return events

    def get_related_markets(self, events):
        markets = []
        event_ids = [event['id'] for event in events]
        i = 0
        while i*CHUNK_SIZE < len(event_ids):
            events_to_fetch = ','.join(event_ids[i*CHUNK_SIZE:(i+1)*CHUNK_SIZE])
            request_url = f'{BASE_URL}events/{events_to_fetch}/markets/?sort=event_id,display_order&limit_by_event=1&with_volumes=true'
            markets += self._client_wrapper(request_url)['markets']
            i += 1
        return markets

    def get_quotes(self, market_ids):
        quotes = []
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

    def _client_wrapper(self, url: str) -> Dict[str, Any]:
        log.info(f'calling url: {url}')
        return requests.get(url).json()
