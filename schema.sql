/* Table for all Crypto exchanges, name, and description if needed */
CREATE TABLE exchanges(exchange_id INTEGER PRIMARY KEY, name TEXT, description TEXT);

INSERT INTO exchanges (exchange_id, name, description) VALUES (1, 'Binance', 'Main Binance Exchange');

/* Table for trading pairs available on a given exchange, symbol is quoteAssetbaseAsset (no underscore) */
CREATE TABLE tradingpairs(tradingpairs_id INTEGER PRIMARY KEY AUTOINCREMENT, 
	symbol TEXT, baseAsset TEXT, quoteAsset TEXT);

/* Table to create Many to Many relationship between tradingpairs and exchanges */
CREATE TABLE exchange_pairs(id INTEGER PRIMARY KEY AUTOINCREMENT, 
	exchange_id INTEGER, tradingpairs_id INTEGER, 
	FOREIGN KEY(exchange_id) REFERENCES exchanges(exchange_id), 
	FOREIGN KEY(tradingpairs_id) REFERENCES tradingpairs(tradingpairs_id));

/* Table to track when something is last updated in the database. First value is 1/1/2021 */
CREATE TABLE updates(id INTEGER PRIMARY KEY AUTOINCREMENT, 
	table_name TEXT, item_id INTEGER, updateTime INTEGER);

INSERT INTO updates(table_name, item_id, updateTime) VALUES ('exchanges', 0, 1609426800000);
INSERT INTO updates(table_name, item_id, updateTime) VALUES ('transactions', 0, 1609426800000);

/* Table to track the source of transaction, exchanges can have several sources each with different endpoints */
CREATE TABLE sources(source_id INTEGER PRIMARY KEY, name TEXT, description TEXT);

INSERT INTO sources (source_id, name, description) VALUES (1, 'Binance Fiat', 'Binance Fiat transactions');

/* Master table of transactions */
CREATE TABLE transactions(transaction_id INTEGER PRIMARY KEY AUTOINCREMENT, 
	buyTime int, crypto TEXT, jpy_price INTEGER, amount INTEGER, transaction_type INTEGER, source_id INTEGER, 
	FOREIGN KEY (source_id) REFERENCES sources (source_id));