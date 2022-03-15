import csv
import datetime
import time
import os
import sqlite3
from sqlite3 import Error
from forex_python.converter import get_rate
from binance import Client, ThreadedWebsocketManager, ThreadedDepthCacheManager

class Transactions:
	def __init__(self):
		self.client = Client(os.environ.get('BINANCE_API_KEY'), os.environ.get('BINANCE_SECRET_KEY'))
		self.transactions = []
		self.purchaseAverages = {}
		self.__initUpdateTimes()

	# looks up the USD value on binance and the exchange rate for that 
	# day to create jpy_price
	def addUSDPurchase(self, buyTime, boughtCrypto, amount):

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
		self.transactions.append([buyTime, boughtCrypto, jpy_price, amount])
		self.__addPurchase(boughtCrypto=boughtCrypto, jpy_price=jpy_price, amount=amount)

	# if the crypto was bought in JPY no conversion is needed
	def addJPYPurchase(self, buyTime, boughtCrypto, jpy_price, amount):
		self.transactions.append([buyTime, boughtCrypto, jpy_price, amount])
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

	def __getDefaultStartTime(self):
		return int(time.mktime(datetime.datetime.strptime("01/01/2021", "%d/%m/%Y").timetuple()) * 1000)

	def __getConnection(self):
		try:
			con = sqlite3.connect('main.db')
		except Error:
			print(Error)

		return con

	def __initUpdateTimes(self):
		con = self.__getConnection()
		cursorObj = con.cursor()
		cursorObj.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='transactions' ''')
		if cursorObj.fetchone()[0]!=1 : 
			cursorObj.execute("CREATE TABLE transactions(id integer PRIMARY KEY AUTOINCREMENT, buyTime int, crypto text, jpy_price int, amount int)")
			self.fiatLastUpdate = self.__getDefaultStartTime
		else: 
			cursorObj.execute("SELECT buyTime FROM transactions ORDER BY buyTime DESC LIMIT 1")
			
			updateTime = cursorObj.fetchone()[0]
			if updateTime is not None:
				self.fiatLastUpdate = updateTime
			else:
				self.fiatLastUpdate = self.__getDefaultStartTime
		
		con.commit()

		con.close()

	# Write transactions to csv
	def writeTransactions(self):
		con = self.__getConnection()
		
		cursorObj = con.cursor()

		print(self.purchaseAverages)

		for j in range(len(self.transactions)):
			row = self.transactions[j]
		#	cursorObj.execute("INSERT INTO transactions (buyTime, crypto, jpy_price, amount) VALUES (" + str(row[0]) + ", '" + str(row[1]) + "', " + str(row[2]) + ", " + str(row[3]) + ");")
		#	con.set_trace_callback(print)
			con.commit()


		con.close()
		with open('transactions.csv', 'w') as f:

			writer = csv.writer(f)
			writer.writerows(self.transactions)


