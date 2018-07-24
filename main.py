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
        # 1. TODO Authenticate using SmarketsClient
        # ...

    def run(self):
        # 2. TODO Fetch the event. Try out ids: 939730, 939734, 939772 or pick yours
        # events = ...
        # 3. TODO Fetch the markets for the event
        # markets = ...

        while True:
            if not self.quote_fetcher or not self.quote_fetcher.is_alive():
                self.quote_fetcher = utils.QuoteFetcher(self.smk_client, markets)
                self.quote_fetcher.start()
            # 4. TODO Start the bot (uncomment following lines)
            # if not self.the_bot or not self.the_bot.is_alive():
                # self.the_bot = example.ExampleBot(self.smk_client, markets)
                # self.the_bot.start()
            log.info('babysitting threads for %s seconds', configuration["misc"]["sleep_interval"])
            time.sleep(configuration["misc"]["sleep_interval"])

def main():
    TradingBot().run()


if __name__ == '__main__':
    main()
