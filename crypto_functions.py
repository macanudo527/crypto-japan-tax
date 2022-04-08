import csv
import datetime
import time
import os
import sqlite3
from sqlite3 import Error
from decimal import Decimal
from forex_python.converter import get_rate
from binance import Client, ThreadedWebsocketManager, ThreadedDepthCacheManager
from dateutil.relativedelta import relativedelta

class CryptoExchange:

	def __init__(self):
		self.tradingpairs = []
		self.userTradingPairs = []

	def getTradingPairs(self):
		return self.tradingpairs

	def getUserTradingPairs(self):
		return self.userTradingPairs

class BinanceExchange(CryptoExchange):

	EXCHANGE_ID = 1
	DIVIDENDS_ID = 3

	def __init__(self):

		# Later these will be taken in and assigned dynamically with init variables, they are hard coded now
		self.client = Client(os.environ.get('BINANCE_API_KEY'), os.environ.get('BINANCE_SECRET_KEY'))
		self.exchangePairs = []
		self.knownPairs = [] 
		self.knownPairTimes = [] # 2d array with rows of symbols, [0] is symbol, [1] is timestamp of last update
		self.knownAsset = set() # We don't want to add a knownAsset twice, so we'll make it a set
		self.systemState = SystemState()
		self.balances = Balances(self.EXCHANGE_ID) #Tracks the wallet snapshot for Binance

		self.__updatePairs()
		self.__updateUserPairs()

	def __getConnection(self):
		try:
			con = sqlite3.connect('main.db')
		except Error:
			print(Error)

		return con

	def __updatePairs(self):

		exchangeInfo = self.client.get_exchange_info()
		con = self.__getConnection()
		cursorObj = con.cursor()

		# STUB Check if it needs updating
		pairsOld = True
		self.exchangePairs = self.getTradingPairs()
		oldPairs = self.exchangePairs

		if pairsOld:

			# Insert new pairs if they are not already in the DB
			for symbol in exchangeInfo['symbols']:
				if symbol['symbol'] not in oldPairs:
					cursorObj.execute("INSERT INTO exchange_pairs_view VALUES (?,?,?,?)",
						(1, symbol['symbol'], symbol['baseAsset'], symbol['quoteAsset']))
					con.commit()
				else:
					oldPairs.remove(symbol['symbol'])

			# Clean DB of oldPairs that are no longer on the exchange
			if len(oldPairs) != 0:
				for oldPair in oldPairs:
					cursorObj.execute("DELETE FROM exchange_pairs"
						"WHERE tradingpairs_id IN ("
						"	SELECT tradingpairs_id FROM tradingpairs"
						"	WHERE symbol = ?", (oldPair))
					cursorObj.commit()

		con.close()

	# STUB for now
	def __updateUserPairs(self):
		pass

	def getTradingPairs(self):
		con = self.__getConnection()
		cursorObj = con.cursor()
		pairs = set()

		# Pull a list of the current symbols registered to this exchange.
		cursorObj.execute("SELECT symbol FROM tradingpairs t "
			"INNER JOIN exchange_pairs ep on ep.tradingpairs_id = t.tradingpairs_id "
			"WHERE ep.exchange_id = 1;")
		rows = cursorObj.fetchall()
		for row in rows:
			pairs.add(row[0])			

		con.close()

		return pairs

	# We track transactions from initial deposits.
	# YET to be implemented: retry with different dates if the number of returned deposits is the limit.
	def getAllDeposits(self):
		self.deposits = Deposits()
		availablePairs = self.getTradingPairs()
		startYear2021 = self.systemState.last_update

		# Times it by 1000 to get milliseconds, subtract one millisecond to get the 
		# absolute last moment of the year.
		endYear2021 = int(datetime.datetime.now().timestamp()) * 1000 - 1

		# Process fiat payments, they do not have a date limit
		fiat_payments = self.client.get_fiat_payments_history(transactionType="0",
			beginTime=startYear2021, endTime=endYear2021)

		if fiat_payments['total'] > 0:
			for deposit in fiat_payments['data']:

				if deposit['status'] != "Failed":

					if deposit['cryptoCurrency'] not in self.knownAsset:

						# search available pairs for ones that end with the given cryptocurrency, 
						# (ie. ones that are quoted in our currency)
						newPairs = [k for k in availablePairs if k.endswith(deposit['cryptoCurrency'])]
						self.knownAsset.add(deposit['cryptoCurrency'])
						for pair in newPairs:
							if pair not in self.knownPairs:
								self.knownPairs.append(pair)
								self.knownPairTimes.append([pair, deposit['cryptoCurrency'], deposit['updateTime']])


					if deposit['fiatCurrency'] == "USD":

						self.transactions.addUSDPurchase(buyTime=int(deposit['updateTime']),
							boughtCrypto=deposit['cryptoCurrency'], amount=deposit['obtainAmount'],
							source=Transactions.BINANCE_FIAT)

					else:

						self.transactions.addJPYPurchase(buyTime=int(deposit['updateTime']),
							boughtCrypto=deposit['cryptoCurrency'], jpy_price=deposit['sourceAmount'],
							amount=deposit['obtainAmount'], source=Transactions.BINANCE_FIAT)	

					self.balances.addBalanceChange(changeTime=int(deposit['updateTime']),
						asset=deposit['cryptoCurrency'], amount=deposit['obtainAmount'])

		# Process crypto deposits
		# YET to be implemented: retry with different dates if the number of returned deposits is the limit.
		
		currentStart = datetime.datetime.fromtimestamp(startYear2021 // 1000) + relativedelta(seconds=+1)

		endYear2021 //= 1000

		# Set the current end to 90 days past currentStart since that is the current limit
		currentEnd =  currentStart + relativedelta(days=+90)
		while int(currentStart.timestamp()) < endYear2021: 
			print("StartTime " + str(int(currentStart.timestamp()) * 1000) + " EndTime " + str(int(currentEnd.timestamp()) * 1000))
			crypto_deposits = self.client.get_deposit_history(startTime=(int(currentStart.timestamp()) * 1000), 
				endTime=(int(currentEnd.timestamp()) * 1000))

			if len(crypto_deposits) > 0:
				for deposit in crypto_deposits:
					if deposit['status'] == 1:
						if deposit['coin'] not in self.knownAsset:
							newPairs = [k for k in availablePairs if k.endswith(deposit['coin'])]
							self.knownAsset.add(deposit['coin'])
							for pair in newPairs:
								if pair not in self.knownPairs:
									self.knownPairs.append(pair)
									self.knownPairTimes.append([pair, deposit['coin'], deposit['insertTime']])

						self.deposits.addExchangeDeposit(insertTime=deposit['insertTime'], coin=deposit['coin'],
							amount=deposit['amount'], txId=deposit['txId'], network=deposit['network'],
							address=deposit['address'], tag=deposit['addressTag'], exchange=self.EXCHANGE_ID, 
							usd_fee=None)

						self.balances.addBalanceChange(changeTime=int(deposit['updateTime']),
							asset=deposit['cryptoCurrency'], amount=deposit['obtainAmount'])


			currentStart = currentEnd + relativedelta(seconds=+1)
			currentEnd = currentStart + relativedelta(days=+90)
			if int(currentEnd.timestamp()) > endYear2021:
				currentEnd = datetime.datetime.fromtimestamp(endYear2021)

	def getAllDividends(self):

		self.income = Income(self.exchangePairs)

		# Pull just one month of data at a time. This allows for 16 assets with daily dividends, 
		# which should be suitable for most users.

		# For now this is STUB for testing, in production it will be pulled from DB
		# start = datetime.datetime.fromtimestamp(1619794804)
		start = datetime.datetime.fromtimestamp(1643434200)
		end =  start + relativedelta(months=+1)

		nowStamp = int(datetime.datetime.now().timestamp()) * 1000 - 1

		while int(start.timestamp()) < nowStamp:
			# Must explicitly declare 500 otherwise we just get 20.
			print("Requesting start=" + str(int(start.timestamp()) * 1000) + " end=" + str(int(end.timestamp()) * 1000))
			dividends = self.client.get_asset_dividend_history(startTime=str(int(start.timestamp()) * 1000), 
				endTime=str(int(end.timestamp()) * 1000), limit=500)
			time.sleep(20)
			# Add condition to reset time frames if total = 500

			# If there are dividends then record them
			if dividends['total'] != "0":
				for dividend in dividends['rows']:
					self.income.addIncome(asset=dividend['asset'], amount=dividend['amount'], 
						divTime=dividend['divTime'], memo=("Binance - " + dividend['enInfo']),
						source_id=Income.BINANCE_DIVIDENDS, transactions=self.transactions)
					time.sleep(0.1)

					self.balances.addBalanceChange(changeTime=dividend['divTime'],
						asset=dividend['asset'], amount=dividend['amount'])

			start = end + relativedelta(seconds=+1)
			end = start + relativedelta(months=+1)
			if int(end.timestamp()) > nowStamp:
				end = datetime.datetime.fromtimestamp(nowStamp)

	def getAllTrades(self):
		self.transactions = Transactions()
		for pair in self.knownPairTimes:
			time.sleep(0.5)
			pairTransactions = self.client.get_my_trades(symbol=pair[0])

			# Subtract the quoteAsset's length from the pair to get baseAsset
			baseAsset = pair[0][:-len(pair[1])]

			if len(pairTransactions) > 0:
				# We've traded a knownAsset into another asset, which is now known.
				self.knownAsset.add(baseAsset)
				print(pairTransactions)
				for transaction in pairTransactions:
					self.transactions.addCryptoPurchase(buyTime=int(transaction['time']), 
						boughtCrypto=baseAsset, amount=transaction['qty'], quoteAsset=pair[1], 
						price=transaction['quoteQty'], source=Transactions.BINANCE_TRADE)

					# Add the baseAsset to the balances
					self.balances.addBalanceChange(buyTime=int(transaction['time']),
						asset=baseAsset, amount=transaction['qty'])

					# Subtract the quoteAsset to the balances
					self.balances.addBalanceChange(buyTime=int(transaction['time']),
						asset=pair[1], amount=-transaction['quoteQty'])

		dust = self.client.get_dust_log()

		for dusting in dust['userAssetDribblets']:
			for dust_detail in dusting['userAssetDribbletDetails']:
				self.transactions.addCryptoPurchase(buyTime=int(dust_detail['operateTime']), 
					boughtCrypto='BNB', amount=dust_detail['transferedAmount'], quoteAsset=dust_detail['fromAsset'],
					price=dust_detail['amount'], source=Transactions.BINANCE_DUST)
				
				# Add BNB to balances
				self.balances.addBalanceChange(buyTime=int(dust_detail['operateTime']),
					asset='BNB', amount=dust_detail['transferedAmount'])

				# Subtract the dusted asset
				self.balances.addBalanceChange(buyTime=int(dust_detail['operateTime']),
					asset=dust_detail['fromAsset'], amount=-dust_detail['amount'])


    # Pulls all transactions from the exchange, starting with deposits, then trades, then pulling savings products
	def getAllTransactions(self):
		self.getAllDeposits()
		self.deposits.writeTransactions()
		self.getAllTrades()
		self.getAllDividends()
		self.transactions.writeTransactions()
		self.income.writeIncome()

class Balances:

	def __init__(self, exchange_id):
		self.changes = []
		self.balances = {}
		self.exchange_id = exchange_id

		# adapter and converter to store decimals in sqlite3, needed to accurately store cryptocurrency balances.
		sqlite3.register_adapter(Decimal, lambda d: str(d))
		sqlite3.register_converter("DECTEXT", lambda d: Decimal(d.decode('ascii')))

		try:
			self.con = sqlite3.connect('main.db', detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
		except Error:
			print(Error)

		# Allows for the aggregation / sum of DECTEXT type columns
		self.con.create_aggregate("decimal_sum", 1, DecimalSum)	

		#Print out SQL for troubleshooting
		self.con.set_trace_callback(print)
	
	def addBalanceChange(self, changeTime, asset, amount):
		self.changes.append([changeTime, asset, amount, self.exchange_id])
		self.balances[asset] = self.balances.get(asset, 0) + amount

# Stubbed class for users
class CryptoUser:
	def getExchanges(self):

		# Authorization is hard coded with system variables now, but in the future, 
		# keys should be added to this init depending on the user.
		return [BinanceExchange()]

class DecimalSum:
	def __init__(self):
		self.sum = None

	def step(self, value):
		if value is None:
			return
		v = Decimal(value)
		if self.sum is None:
			self.sum = v
		else:
			self.sum += v

	def finalize(self):
		return None if self.sum is None else str(self.sum)

class Deposits:
	def __init__(self):
		self.deposits = []

		# adapter and converter to store decimals in sqlite3, needed to accurately store cryptocurrency balances.
		sqlite3.register_adapter(Decimal, lambda d: str(d))
		sqlite3.register_converter("DECTEXT", lambda d: Decimal(d.decode('ascii')))

		try:
			self.con = sqlite3.connect('main.db', detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
		except Error:
			print(Error)

		# Allows for the aggregation / sum of DECTEXT type columns
		self.con.create_aggregate("decimal_sum", 1, DecimalSum)	

		#Print out SQL for troubleshooting
		self.con.set_trace_callback(print)

	def addExchangeDeposit(self, insertTime, coin, amount, txId, network, address, tag, exchange, usd_fee):
		
		# In the future, this may include some logic to look up fees.

		# First, look up the destination type based on exchange id
		cursorObj = self.con.cursor()

		cursorObj.execute("SELECT destination_type_id from destination_types "
			"WHERE exchange_id = ? "
			"AND network = ?;", (exchange, network))
		self.con.commit()

		destination_type = cursorObj.fetchone()
		if destination_type is None:
			cursorObj.execute("INSERT INTO destination_types (name, network, exchange_id) VALUES "
				"((SELECT name FROM exchanges WHERE exchange_id = :exchange) || ' ' || :network, "
				":network, :exchange)",
				{'network': network, 'exchange': exchange})
			self.con.commit()
			destination_type = cursorObj.lastrowid
		else:
			destination_type = destination_type[0]

		self.deposits.append([insertTime, coin, amount, txId, network, address, tag, destination_type, usd_fee])

	def writeTransactions(self):
		cursorObj = self.con.cursor()

		for j in range(len(self.deposits)):
			row = self.deposits[j]
			cursorObj.execute("SELECT destination_id FROM destinations "
				"WHERE network = ? "
				"AND address = ? "
				"AND tag = ? "
				"AND type_id = ?;",(row[4], row[5], row[6], row[7]))
			self.con.commit()

			destination = cursorObj.fetchone()
			# If the destination doesn't exist create it and retrieve the id
			if destination is None:
				cursorObj.execute("INSERT INTO destinations (name, network, address, tag, type_id) VALUES ("
					"?,?,?,?,?)", (str(row[7]) + row[5], row[4], row[5], row[6], row[7]))
				self.con.commit()
				destination_id = cursorObj.lastrowid
			else:
				destination_id = destination[0]


			cursorObj.execute("INSERT INTO transfers (insertTime, crypto, amount, tx_id, destination_id, origin_id, "
				"usd_cost, jpy_cost) VALUES (?,?,?,?,?,?,?,?)", (str(row[0]), str(row[1]), str(row[2]),
				str(row[3]), destination_id, None, None, None))
			self.con.commit()
			cursorObj.execute("INSERT INTO updates(table_name, item_id, updateTime) VALUES ('deposits', 0, "
				+ str(row[0]) + ");")
			self.con.commit()


		self.con.close()

class Income:

	BINANCE_DIVIDENDS = 3

	def __init__(self, exchangePairs):
		self.income = []
		self.exchangePairs = exchangePairs


		# adapter and converter to store decimals in sqlite3, needed to accurately store cryptocurrency balances.
		sqlite3.register_adapter(Decimal, lambda d: str(d))
		sqlite3.register_converter("DECTEXT", lambda d: Decimal(d.decode('ascii')))

		try:
			self.con = sqlite3.connect('main.db', detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
		except Error:
			print(Error)

		# Allows for the aggregation / sum of DECTEXT type columns
		self.con.create_aggregate("decimal_sum", 1, DecimalSum)	

		#Print out SQL for troubleshooting
		self.con.set_trace_callback(print)

		self.client = Client(os.environ.get('BINANCE_API_KEY'), os.environ.get('BINANCE_SECRET_KEY'))

		self.stableCoins = set(["USDT", "BUSD", "USDC", "UST", "DAI", "TUSD", "USDP", "USDN", 
			"FEI", "FRAX", "LUSD", "HUSD", "GUSD", "OUSD", "SUSD", "CUSD"])

		self.income = []

	# Add income without known USD value
	def addIncome(self, asset, amount, divTime, memo, source_id, transactions):

		divTime = int(divTime)
		endTime = divTime + 600000

		print("Adding Income start = " + str(divTime) + " end = " + str(endTime) + " " + asset)

		if asset == "BETH":
			bethPrice = []
			# Sometimes there isn't a minute candle, so find the next available one
			# This is only necessary for distributions since there is no trading taking place
			while not bethPrice:
				bethPrice = self.client.get_historical_klines(symbol="BETHETH", interval="1m", 
					start_str=divTime, end_str=endTime)
				divTime += 600000
				endTime += 600000
				time.sleep(0.05)

			bethPrice = bethPrice[0][1]
			
			ethPrice = self.client.get_historical_klines(symbol="ETHUSDT", interval="1m", 
				start_str=divTime, end_str=endTime)[0][1]
			usd_value = Decimal(bethPrice) * Decimal(ethPrice) * Decimal(amount)

		elif asset not in self.stableCoins:
			assetSymbol = asset + "USDT"
			if assetSymbol in self.exchangePairs:
				usdPrice = []

				# Sometimes dividend income is not being traded the moment it was distributed
				while not usdPrice:
					usdPrice = self.client.get_historical_klines(symbol=assetSymbol, interval="1m", 
						start_str=divTime, end_str=endTime)

					divTime += 600000
					endTime += 600000
					time.sleep(0.05)

				usdPrice = usdPrice[0][1]
			else:
				usdPrice = None

				usd_value = Decimal(usdPrice) * Decimal(amount) if usdPrice is not None else None
		else:
			usd_value = Decimal(amount)

		self.addIncomeWithUSD(asset, amount, divTime, memo, usd_value, source_id)

		transactions.addUSDPurchaseREVISED(buyTime=divTime, boughtCrypto=asset, amount=amount, 
			usd_price=usd_value, source=source_id)

	def addIncomeWithUSD(self, asset, amount, divTime, memo, usd_value, source_id):

		self.income.append([asset, amount, divTime, memo, usd_value, source_id])

	def writeIncome(self):
		cursorObj = self.con.cursor()

		for j in range(len(self.income)):
			row = self.income[j]
			cursorObj.execute("INSERT INTO income (asset, amount, divTime, usd_value, source_id) VALUES "
				"(?,?,?,?,?);",(str(row[0]), str(row[1]), str(row[2]), str(row[3]), str(row[4])))
			cursorObj.execute("INSERT INTO updates(table_name, item_id, updateTime) VALUES ('income', 0, "
				+ str(row[2]) + ");")
			self.con.commit()


		self.con.close()




class SystemState:
	def __init__(self):
		self.__initUpdateTimes()

	def __getConnection(self):
		try:
			con = sqlite3.connect('main.db')
		except Error:
			print(Error)

		return con

	def __initUpdateTimes(self):
		con = self.__getConnection()
		cursorObj = con.cursor()
		defaultUpdate = int(datetime.datetime(2021,1,1,0,0,0,0).timestamp()) * 1000
		cursorObj.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='exchanges' ''')
		if cursorObj.fetchone()[0]!=1 : 

			# Pull in master schema and default values for the database
			with open('schema.sql') as f:
				con.executescript(f.read())
			self.last_update = defaultUpdate
		else: 
			cursorObj.execute("SELECT updateTime FROM updates ORDER BY updateTime DESC LIMIT 1")
			
			updateTime = cursorObj.fetchone()
			if updateTime is not None:
				self.last_update = updateTime[0]
			else:
				self.last_update = defaultUpdate
		
		con.commit()

		con.close()	

class Transactions:
	BINANCE_FIAT = 1
	BINANCE_TRADE = 2
	BINANCE_DUST = 4
	BUY = 0
	SELL = 1

	def __init__(self):
		self.client = Client(os.environ.get('BINANCE_API_KEY'), os.environ.get('BINANCE_SECRET_KEY'))
		self.transactions = []
		self.purchaseAverages = {}
		self.stableCoins = set(["USDT", "BUSD", "USDC", "UST", "DAI", "TUSD", "USDP", "USDN", 
			"FEI", "FRAX", "LUSD", "HUSD", "GUSD", "OUSD", "SUSD", "CUSD"])

		# adapter and converter to store decimals in sqlite3, needed to accurately store cryptocurrency balances.
		sqlite3.register_adapter(Decimal, lambda d: str(d))
		sqlite3.register_converter("DECTEXT", lambda d: Decimal(d.decode('ascii')))

		try:
			self.con = sqlite3.connect('main.db', detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
		except Error:
			print(Error)

		# Allows for the aggregation / sum of DECTEXT type columns
		self.con.create_aggregate("decimal_sum", 1, DecimalSum)

		#Print out SQL for troubleshooting
		self.con.set_trace_callback(print)

	# looks up the USD value on binance and the exchange rate for that 
	# day to create jpy_price
	def addUSDPurchase(self, buyTime, boughtCrypto, amount, source):

		# This will be CACHED in the DB in the future
		t = datetime.datetime.fromtimestamp(buyTime / 1000)
		jpy_rate = Decimal(get_rate("USD", "JPY", t))
		endTime = buyTime + 60000

		if boughtCrypto == "BETH":
			bethPrice = self.client.get_historical_klines(symbol="BETHETH", interval="1m", 
				start_str=buyTime, end_str=endTime)[0][1]
			ethPrice = self.client.get_historical_klines(symbol="ETHUSDT", interval="1m", 
				start_str=buyTime, end_str=endTime)[0][1]
			usdTotal = Decimal(bethPrice) * Decimal(ethPrice) * Decimal(amount)

		elif boughtCrypto not in self.stableCoins:
			boughtSymbol = boughtCrypto + "USDT"

			usdPrice = self.client.get_historical_klines(symbol=boughtSymbol, interval="1m", 
				start_str=buyTime, end_str=endTime)[0][1]
			usdTotal = Decimal(usdPrice) * Decimal(amount)
		else:
			usdTotal = Decimal(amount)

		jpy_price = usdTotal * jpy_rate 
		self.transactions.append([buyTime, boughtCrypto, usdTotal, jpy_price, amount, Transactions.BUY, source])
		# self.__addPurchase(boughtCrypto=boughtCrypto, jpy_price=jpy_price, amount=amount)

	def addUSDPurchaseREVISED(self, buyTime, boughtCrypto, amount, usd_price, source):

		# This will be CACHED in the DB in the future
		t = datetime.datetime.fromtimestamp(buyTime / 1000)
		jpy_rate = Decimal(get_rate("USD", "JPY", t))
		endTime = buyTime + 60000
		
		jpy_price = usd_price * jpy_rate if usd_price is not None else None
		self.transactions.append([buyTime, boughtCrypto, usd_price, jpy_price, amount, Transactions.BUY, source])
		# self.__addPurchase(boughtCrypto=boughtCrypto, jpy_price=jpy_price, amount=amount)			

	def addCryptoPurchase(self, buyTime, boughtCrypto, amount, quoteAsset, price, source):
		
		# Add the purchase, but we also have to add a sale below since this is not a stable coin
		self.addUSDPurchase(buyTime, boughtCrypto, amount, source)

		# This will be CACHED in the DB in the future
		t = datetime.datetime.fromtimestamp(buyTime / 1000)
		jpy_rate = Decimal(get_rate("USD", "JPY", t))

		endTime = buyTime + 60000

		# Beaconed Eth is not paired directly with a stable coin, so to calculate total we need to
		# pull Eth price and multiply it by the beth price.
		if quoteAsset == "BETH":
			bethPrice = self.client.get_historical_klines(symbol="BETHETH", interval="1m", 
				start_str=buyTime, end_str=endTime)[0][1]
			ethPrice = self.client.get_historical_klines(symbol="ETHUSDT", interval="1m", 
				start_str=buyTime, end_str=endTime)[0][1]
			usdTotal = Decimal(bethPrice) * Decimal(ethPrice) * Decimal(amount)			 

		elif quoteAsset not in self.stableCoins:
			quoteSymbol = quoteAsset + "USDT"

			usdPrice = self.client.get_historical_klines(symbol=quoteSymbol, interval="1m", 
				start_str=buyTime, end_str=endTime)[0][1]
			usdTotal = Decimal(usdPrice) * Decimal(price)
		else:
			usdTotal = Decimal(price)

		jpy_price = usdTotal * jpy_rate
		self.transactions.append([buyTime, quoteAsset, usdTotal, jpy_price, price, Transactions.SELL, source])
		# self.__addSale(quoteAsset=quoteAsset, jpy_price=jpy_price, price=price)	

	# if the crypto was bought in JPY no conversion is needed
	def addJPYPurchase(self, buyTime, boughtCrypto, jpy_price, amount, source):

		# This will be CACHED in the DB in the future
		t = datetime.datetime.fromtimestamp(buyTime / 1000)
		jpy_rate = Decimal(get_rate("USD", "JPY", t))
		usd_price = Decimal(jpy_price) / jpy_rate	

		self.transactions.append([buyTime, boughtCrypto, usd_price, jpy_price, amount, Transactions.BUY, source])
		# self.__addPurchase(boughtCrypto=boughtCrypto, jpy_price=jpy_price, amount=amount)

	# Add up the total purchases in order to calculate total average
	def __addPurchase(self, boughtCrypto, jpy_price, amount):
		billion = 1000000000

		if self.purchaseAverages.get(boughtCrypto) is not None:
			self.purchaseAverages[boughtCrypto] += int(float(jpy_price) * billion)
			self.purchaseAverages[boughtCrypto + "_AMOUNT"] += int(float(amount) * billion)
		else:
			self.purchaseAverages[boughtCrypto] = int(float(jpy_price) * billion)
			self.purchaseAverages[boughtCrypto + "_AMOUNT"] = int(float(amount) * billion)

	def __addSale(self, quoteAsset, jpy_price, price):
		pass

	def __getConnection(self):
		try:
			con = sqlite3.connect('main.db')
		except Error:
			print(Error)

		return con

	# Write transactions to csv
	def writeTransactions(self):
		 # con = self.__getConnection()
		
		cursorObj = self.con.cursor()

		print(self.purchaseAverages)

		for j in range(len(self.transactions)):
			row = self.transactions[j]
			cursorObj.execute("INSERT INTO transactions (buyTime, crypto, usd_price, jpy_price, amount, transaction_type, source_id) VALUES ("
			 	+ str(row[0]) + ", '" + str(row[1]) + "', " + str(row[2]) + ", " + str(row[3]) 
			 	+ ", " + str(row[4]) + ", " + str(row[5]) + ", " + str(row[6]) + ");")
			cursorObj.execute("INSERT INTO updates(table_name, item_id, updateTime) VALUES ('transactions', 0, "
				+ str(row[0]) + ");")
			self.con.commit()


		self.con.close()
		with open('transactions.csv', 'a') as f:

			writer = csv.writer(f)
			writer.writerows(self.transactions)
