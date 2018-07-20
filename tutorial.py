from pprint import pprint

import client
from utils import LiveQuotePlotter, QuoteFetcher

# instantiate client: single instance per session
# copy the template from configuration_template.toml and fill it with
# your credentials!
client = client.SmarketsClient()

# do initial authentication
client.init_session()

# pick some market and contract
market_id = '6672405'
contract_id = '21490523'

# place some bets
# don't worry about the price: the buy price is set to be very high
# it's very unlikely someone will match you :)

client.place_bet(
    market_id,    # market id
    contract_id,  # contract id
    50,           # percentage price * 10**4, here: 0.5% / 200 decimal / 19900 american
    500000,       # quantity: total stake * 10**4, here: 50 GBP. Your buy order locks 0.25 GBP, as
                  #      0.25 GBP * 200 = 50 GBP
    'buy',        # order side: buy or sell
)


# lets get the orders now!
pprint(client.get_orders(states=['created', 'filled', 'partial']))

pprint(client.get_accounts())
pprint(client.get_account_activity(market_id))

# eeh, changed my mind
# client.cancel_bet('202547466272702478')

# Lets run the quote fetching!
QuoteFetcher(client).run()

# Show me live prices
LiveQuoteFetcher(client).run(market_id)
