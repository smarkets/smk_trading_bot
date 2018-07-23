import datetime
import logging
import time
from logging.config import fileConfig

import utils
import example
from config import configuration
from client import SmarketsClient


fileConfig('logging.config', disable_existing_loggers=False)

log = logging.getLogger(__name__)

class TradingBot:
    def __init__(self):
        self.quote_fetcher = None
        self.authenticator = None
        self.the_bot = None
        self.smk_client = SmarketsClient()
        self.smk_client.init_session()

    def run(self):
        events = self.smk_client.get_events(['940308']) # 939730
        print(events)
        markets = self.smk_client.get_related_markets(events)
        while True:
            if not self.quote_fetcher or not self.quote_fetcher.is_alive():
                self.quote_fetcher = utils.QuoteFetcher(self.smk_client, markets)
                self.quote_fetcher.start()
            if not self.the_bot or not self.the_bot.is_alive():
                self.the_bot = example.ExampleBot(self.smk_client, markets)
                self.the_bot.start()
            log.info('babysitting threads for %s seconds', configuration["misc"]["sleep_interval"])
            time.sleep(configuration["misc"]["sleep_interval"])

def main():
    TradingBot().run()


if __name__ == '__main__':
    main()
