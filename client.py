import datetime
import logging
import sqlite3
import threading
from typing import Any, Dict, List
from collections import defaultdict

import requests

from config import configuration

log = logging.getLogger(__name__)

class OrderPlaceError(Exception):
    pass


class SmarketsClient:
    def __init__(self):
        self.auth_token = None

    def _auth_headers(self):
        return {'Authorization': 'Session-Token ' + self.auth_token}

    def init_session(self):
        log.info('initiating session')
        self.auth_token = configuration['auth'].get('auth_token')
        if not self.auth_token:
            response = requests.post(
                f'{configuration["api"]["base_url"]}sessions/',
                json={
                    'username': configuration['auth']['login'],
                    'password': configuration['auth']['password'],
                },
            ).json()
            self.auth_token = response.get('token')
        log.info('auth token: %s', self.auth_token)

    def place_order(
        self,
        market_id,
        contract_id,
        price,
        quantity,
        side,
    ):
        log.info(
            'placing order: m_id %s: c_id %s\t %s %s @ %s',
            market_id,
            contract_id,
            side,
            quantity,
            price,
        )
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
        )
        response_body = response.json()
        if response.status_code != 200:
            raise OrderPlaceError(response_body.get('error_type'))
        log.info(
            f'''order placed: m_id {market_id}: c_id {contract_id} \t {side} {quantity} @ {price}|'''
            f'''balance:{response_body["available_balance"]} executed:{response_body["total_executed_quantity"]}'''
            f''' exposure:{response_body["exposure"]}'''
        )

    def cancel_order(self, order_id):
        requests.delete(
            f'{configuration["api"]["base_url"]}orders/{order_id}/',
            headers=self._auth_headers(),
        )

    def get_orders(self, states):
        orders = []
        states_to_fetch = '&'.join([f'states={state}' for state in states])
        next_page = f'?limit={configuration["api"]["chunk_size"]}&{states_to_fetch}'
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

    def get_events(self, event_ids: List[str]):
        event_ids_filter = '&'.join([f'ids={event_id}' for event_id in event_ids])
        request_url = f'{configuration["api"]["base_url"]}events/?{event_ids_filter}'
        return self._client_wrapper(request_url).get('events')

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
        response = requests.get(
            f'{configuration["api"]["base_url"]}accounts/', headers=self._auth_headers()
        ).json()
        return response

    def _client_wrapper(self, url: str) -> Dict[str, Any]:
        log.info(f'calling url: {url}')
        return requests.get(url).json()
