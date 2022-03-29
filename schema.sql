/* Table for all Crypto exchanges, name, and description if needed */
CREATE TABLE exchanges(exchange_id INTEGER PRIMARY KEY, name TEXT, description TEXT);

INSERT INTO exchanges (exchange_id, name, description) VALUES (1, 'Binance', 'Main Binance Exchange');

/* Table for trading pairs available on a given exchange, symbol is quoteAssetbaseAsset (no underscore) */
CREATE TABLE tradingpairs(tradingpairs_id INTEGER PRIMARY KEY AUTOINCREMENT, 
	symbol TEXT UNIQUE, baseAsset TEXT, quoteAsset TEXT);

/* Table to create Many to Many relationship between tradingpairs and exchanges */
CREATE TABLE exchange_pairs(id INTEGER PRIMARY KEY AUTOINCREMENT, 
	exchange_id INTEGER, tradingpairs_id INTEGER, 
	FOREIGN KEY(exchange_id) REFERENCES exchanges(exchange_id), 
	FOREIGN KEY(tradingpairs_id) REFERENCES tradingpairs(tradingpairs_id));

/* View that enables Many to Many relationship to be created when we insert exchange pairs */
CREATE VIEW exchange_pairs_view AS
	SELECT
		E.exchange_id, TP.symbol, TP.baseAsset, TP.quoteAsset
	FROM exchange_pairs AS EP 
		INNER JOIN exchanges AS E ON e.exchange_id = EP.exchange_id
		INNER JOIN tradingpairs AS TP ON tp.tradingpairs_id = EP.tradingpairs_id;

/* 	Trigger adding the many to many relationship when inserting an exchange pair 
	
	Must insert exchange pair INTO the *view*:
		INSERT INTO exchange_pairs_view VALUES (exchange_id, symbol, baseAsset, quoteAsset)
*/
CREATE TRIGGER exchange_pairs_view_insert INSTEAD OF INSERT ON exchange_pairs_view
BEGIN
	INSERT OR IGNORE INTO tradingpairs (symbol, baseAsset, quoteAsset) VALUES 
		(NEW.symbol, NEW.baseAsset, NEW.quoteAsset);
	INSERT OR IGNORE INTO exchange_pairs (exchange_id, tradingpairs_id) VALUES
		(NEW.exchange_id, 
		(SELECT tradingpairs_id FROM tradingpairs WHERE symbol = NEW.symbol));
END;

/* Delete the many to many linking db entry if tradingpair is deleted for some reason */
CREATE TRIGGER delete_trading_pair_relationship DELETE ON tradingpairs
BEGIN
	DELETE FROM exchange_pairs WHERE tradingpairs_id = OLD.tradingpairs_id;
END;

/* Table to track when something is last updated in the database. First value is 1/1/2021 */
CREATE TABLE updates(id INTEGER PRIMARY KEY AUTOINCREMENT, 
	table_name TEXT, item_id INTEGER, updateTime INTEGER);

INSERT INTO updates(table_name, item_id, updateTime) VALUES ('exchanges', 0, 1609426800000);
INSERT INTO updates(table_name, item_id, updateTime) VALUES ('transactions', 0, 1609426800000);

/* Table to track the source of transaction, exchanges can have several sources each with different endpoints */
CREATE TABLE sources(source_id INTEGER PRIMARY KEY, name TEXT, description TEXT);

INSERT INTO sources (source_id, name, description) VALUES (1, 'Binance Fiat', 'Binance Fiat transactions');
INSERT INTO sources (source_id, name, description) VALUES (2, 'Binance Trades', 'Binance Spot Trade transactions');

/* Master table of transactions */
CREATE TABLE transactions(transaction_id INTEGER PRIMARY KEY AUTOINCREMENT, 
	buyTime INTEGER, crypto TEXT, usd_price DECTEXT, jpy_price DECTEXT, amount DECTEXT, transaction_type INTEGER, 
	source_id INTEGER, 
	FOREIGN KEY (source_id) REFERENCES sources (source_id));

CREATE TABLE transfers(transfer_id INTEGER PRIMARY KEY AUTOINCREMENT, insertTime INTEGER, crypto TEXT, 
	amount DECTEXT, destination_id INTEGER, origin_id INTEGER, jpy_cost DECTEXT, usd_cost DECTEXT)