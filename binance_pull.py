import os
import time
import datetime
from forex_python.converter import get_rate
from binance import Client, ThreadedWebsocketManager, ThreadedDepthCacheManager

client = Client(os.environ.get('BINANCE_API_KEY'), os.environ.get('BINANCE_SECRET_KEY'))


start = datetime.datetime.strptime("01/02/2021", "%d/%m/%Y")
end = datetime.datetime.strptime("01/04/2022", "%d/%m/%Y")
startYear2021 = int(datetime.datetime.timestamp(start)) * 1000
endYear2021 = int(datetime.datetime.timestamp(end)) * 1000

fiat_payments = client.get_fiat_payments_history(transactionType="0",
	beginTime=startYear2021, endTime=endYear2021)

# print(fiat_payments)

first_fiat = fiat_payments['data'][0]

print(first_fiat)

boughtCrypto = first_fiat['cryptoCurrency']

if first_fiat['fiatCurrency'] == "USD":
	t = datetime.datetime.fromtimestamp(buyTime)
	jpy_rate = get_rate("USD", "JPY", t)	
	buyTime = first_fiat['updateTime']
	endTime = buyTime + 60000

	boughtSymbol = boughtCrypto + "USDT"


	usdPrice = client.get_historical_klines(symbol=boughtSymbol, interval="1m", 
		start_str=buyTime, end_str=endTime)[0][1]

	print(usdPrice * jpy_rate)
else:
	totalUnits = first_fiat['obtainAmount']
	costPerUnit = float(first_fiat['sourceAmount']) / float(totalUnits)
	print(totalUnits + boughtCrypto + " at " + str(costPerUnit))

print(client.get_asset_balance(asset='BTC'))

# print("Binance API " + os.environ.get('BINANCE_API_KEY') + "\n")
# print("Binance Secret " + os.environ.get('BINANCE_SECRET_KEY') + "\n")