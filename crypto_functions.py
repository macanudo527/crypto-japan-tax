import csv
import datetime
import time
import os
import sqlite3
from sqlite3 import Error
from forex_python.converter import get_rate
from binance import Client, ThreadedWebsocketManager, ThreadedDepthCacheManager

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
		cursorObj.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='exchanges' ''')
		if cursorObj.fetchone()[0]!=1 : 

			# Pull in master schema and default values for the database
			with open('schema.sql') as f:
				con.executescript(f.read())
			self.last_update = 1609426800000 # timestamp for 1/1/2021
		else: 
			cursorObj.execute("SELECT updateTime FROM updates ORDER BY updateTime DESC LIMIT 1")
			
			updateTime = cursorObj.fetchone()
			if updateTime is not None:
				self.last_update = updateTime[0]
			else:
				self.last_update = 1609426800000
		
		con.commit()

		con.close()		




class ExchangeInfo:
	BINANCE = 1

	def __init__(self):
		self.client = Client(os.environ.get('BINANCE_API_KEY'), os.environ.get('BINANCE_SECRET_KEY'))

#	def updatePairs(self):


class Transactions:
	BINANCE_FIAT = 1
	BUY = 0
	SELL = 0

	def __init__(self):
		self.client = Client(os.environ.get('BINANCE_API_KEY'), os.environ.get('BINANCE_SECRET_KEY'))
		self.transactions = []
		self.purchaseAverages = {}

	# looks up the USD value on binance and the exchange rate for that 
	# day to create jpy_price
	def addUSDPurchase(self, buyTime, boughtCrypto, amount, source):

		t = datetime.datetime.fromtimestamp(buyTime / 1000)
		jpy_rate = get_rate("USD", "JPY", t)
		endTime = buyTime + 60000

		if boughtCrypto != "USDT":
			boughtSymbol = boughtCrypto + "USDT"

			usdPrice = self.client.get_historical_klines(symbol=boughtSymbol, interval="1m", 
				start_str=buyTime, end_str=endTime)[0][1]
			usdTotal = float(usdPrice) * float(amount)
		else:
			usdTotal = amount

		jpy_price = float(usdTotal) * float(jpy_rate) 
		self.transactions.append([buyTime, boughtCrypto, jpy_price, amount, Transactions.BUY, source])
		self.__addPurchase(boughtCrypto=boughtCrypto, jpy_price=jpy_price, amount=amount)

	# if the crypto was bought in JPY no conversion is needed
	def addJPYPurchase(self, buyTime, boughtCrypto, jpy_price, amount, source):
		self.transactions.append([buyTime, boughtCrypto, jpy_price, amount, Transactions.BUY, source])
		self.__addPurchase(boughtCrypto=boughtCrypto, jpy_price=jpy_price, amount=amount)

	# Add up the total purchases in order to calculate total average
	def __addPurchase(self, boughtCrypto, jpy_price, amount):
		billion = 1000000000

		if self.purchaseAverages.get(boughtCrypto) is not None:
			self.purchaseAverages[boughtCrypto] += int(float(jpy_price) * billion)
			self.purchaseAverages[boughtCrypto + "_AMOUNT"] += int(float(amount) * billion)
		else:
			self.purchaseAverages[boughtCrypto] = int(float(jpy_price) * billion)
			self.purchaseAverages[boughtCrypto + "_AMOUNT"] = int(float(amount) * billion)

	def __getConnection(self):
		try:
			con = sqlite3.connect('main.db')
		except Error:
			print(Error)

		return con

	# Write transactions to csv
	def writeTransactions(self):
		con = self.__getConnection()
		
		cursorObj = con.cursor()

		print(self.purchaseAverages)

		for j in range(len(self.transactions)):
			row = self.transactions[j]
			cursorObj.execute("INSERT INTO transactions (buyTime, crypto, jpy_price, amount, transaction_type, source_id) VALUES ("
			 	+ str(row[0]) + ", '" + str(row[1]) + "', " + str(row[2]) + ", " + str(row[3]) 
			 	+ ", " + str(row[4]) + ", " + str(row[5]) + ");")
			cursorObj.execute("INSERT INTO updates(table_name, item_id, updateTime) VALUES ('transactions', 0, "
				+ str(row[0]) + ");")
			con.commit()


		con.close()
		with open('transactions.csv', 'a') as f:

			writer = csv.writer(f)
			writer.writerows(self.transactions)


