import logging
from logging.config import fileConfig
import time

import utils
from config import configuration
from client import SmarketsClient


fileConfig('logging.config', disable_existing_loggers=False)

log = logging.getLogger(__name__)

class TradingBot:
    def __init__(self):
        self.quote_fetcher = None
        self.authenticator = None
        self.smk_client = SmarketsClient()

    def run(self):
        while True:
            if not self.quote_fetcher or not self.quote_fetcher.is_alive():
                self.quote_fetcher = utils.QuoteFetcher(self.smk_client)
                self.quote_fetcher.start()
            if not self.authenticator or not self.authenticator.is_alive():
                self.authenticator = utils.Authenticator(self.smk_client)
                self.authenticator.start()
            # if not self.the_bot or not self.the_bot.is_alive():
                # self.the_bot = trading.DumbBot(self.smk_client)
                # self.the_bot.start()
            log.info('babysitting threads for %s seconds', configuration["misc"]["sleep_interval"])
            time.sleep(configuration["misc"]["sleep_interval"])

def main():
    TradingBot().run()


if __name__ == '__main__':
    main()
