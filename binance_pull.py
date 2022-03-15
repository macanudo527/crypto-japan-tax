import os
import time
import datetime
import csv
from forex_python.converter import get_rate
from binance import Client, ThreadedWebsocketManager, ThreadedDepthCacheManager
from crypto_functions import Transactions

client = Client(os.environ.get('BINANCE_API_KEY'), os.environ.get('BINANCE_SECRET_KEY'))
transactions = Transactions()

end = datetime.datetime.strptime("01/01/2022", "%d/%m/%Y")
startYear2021 = transactions.fiatLastUpdate
endYear2021 = int(datetime.datetime.timestamp(end)) * 1000

# Process fiat payments
fiat_payments = client.get_fiat_payments_history(transactionType="0",
	beginTime=startYear2021, endTime=endYear2021)

if fiat_payments['total'] > 0:
	for deposit in fiat_payments['data']:

		if deposit['status'] != "Failed":

			if deposit['fiatCurrency'] == "USD":

				transactions.addUSDPurchase(buyTime=int(deposit['updateTime']),
					boughtCrypto=deposit['cryptoCurrency'], amount=deposit['obtainAmount'])

			else:

				transactions.addJPYPurchase(buyTime=int(deposit['updateTime']),
					boughtCrypto=deposit['cryptoCurrency'], jpy_price=deposit['sourceAmount'],
					amount=deposit['obtainAmount'])

transactions.writeTransactions()

print(client.get_asset_balance(asset='BTC'))

# print("Binance API " + os.environ.get('BINANCE_API_KEY') + "\n")
# print("Binance Secret " + os.environ.get('BINANCE_SECRET_KEY') + "\n")