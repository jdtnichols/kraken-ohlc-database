CREATE SCHEMA kraken;

USE kraken;

CREATE TABLE ethusd_raw (
	trade_id INT  NOT NULL AUTO_INCREMENT PRIMARY KEY,
    trade_timestamp INT,
    price DOUBLE,
   volume DOUBLE
);

CREATE TABLE xbtusd_raw (
	trade_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
	trade_timestamp INT,
	price DOUBLE,
	volume DOUBLE
);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/ethusd_id.csv' INTO TABLE ethusd_raw
	COLUMNS TERMINATED BY ','
    LINES TERMINATED BY '\r\n';

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/xbtusd_id.csv' INTO TABLE xbtusd_raw
	COLUMNS TERMINATED BY ','
    LINES TERMINATED BY '\r\n';
    
CREATE TABLE ethusd_all_trades (
	trade_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
	trade_timestamp INT,
	datetime_s CHAR (19),
	price DOUBLE,
	volume DOUBLE
);
    
INSERT INTO ethusd_all_trades (
	SELECT
	trade_id,
    trade_timestamp,
    FROM_UNIXTIME(trade_timestamp, '%Y-%m-%d %H:%i:%s'),
	price,
	volume
	FROM ethusd_raw
);

CREATE TABLE xbtusd_all_trades (
	trade_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
	trade_timestamp INT,
	datetime_s CHAR (19),
	price DOUBLE,
	volume DOUBLE
);

INSERT INTO xbtusd_all_trades (
	SELECT
	trade_id,
    trade_timestamp,
    FROM_UNIXTIME(trade_timestamp, '%Y-%m-%d %H:%i:%s'),
	price,
	volume
	FROM xbtusd_raw
);

CREATE TABLE ethusd_day (
	date CHAR (10),
	price_open DOUBLE,
    price_high DOUBLE,
    price_low DOUBLE,
    price_close DOUBLE,
    price_avg DOUBLE,
    num_trades INT,
	volume_in_dollars DOUBLE
);

SET SESSION group_concat_max_len = 40000000;

INSERT INTO ethusd_day (
	SELECT
	DATE_FORMAT(datetime_s, '%Y-%m-%d'),
	SUBSTRING_INDEX( GROUP_CONCAT(CAST(price AS CHAR) ORDER BY datetime_s), ',', 1 ),
	MAX(price),
	MIN(price),
	SUBSTRING_INDEX( GROUP_CONCAT(CAST(price AS CHAR) ORDER BY datetime_s DESC), ',', 1 ),
    AVG(price),
	COUNT(*),
	SUM(price*volume)
	FROM ethusd_all_trades
	GROUP BY DATE_FORMAT(datetime_s, '%Y-%m-%d')
	ORDER BY DATE_FORMAT(datetime_s, '%Y-%m-%d') ASC
);

CREATE TABLE xbtusd_day (
	date CHAR (10),
	price_open DOUBLE,
    price_high DOUBLE,
    price_low DOUBLE,
    price_close DOUBLE,
    price_avg DOUBLE,
    num_trades INT,
	volume_in_dollars DOUBLE
);

INSERT INTO xbtusd_day (
	SELECT
	DATE_FORMAT(datetime_s, '%Y-%m-%d'),
	SUBSTRING_INDEX( GROUP_CONCAT(CAST(price AS CHAR) ORDER BY datetime_s), ',', 1 ),
	MAX(price),
	MIN(price),
	SUBSTRING_INDEX( GROUP_CONCAT(CAST(price AS CHAR) ORDER BY datetime_s DESC), ',', 1 ),
    AVG(price),
	COUNT(*),
	SUM(price*volume)
	FROM xbtusd_all_trades
	GROUP BY DATE_FORMAT(datetime_s, '%Y-%m-%d')
	ORDER BY DATE_FORMAT(datetime_s, '%Y-%m-%d') ASC
);

CREATE TABLE ethusd_minute (
	datetime_min CHAR (16),
	price_open DOUBLE,
    price_high DOUBLE,
    price_low DOUBLE,
    price_close DOUBLE,
    price_avg DOUBLE,
    num_trades INT,
	volume_in_dollars DOUBLE
);

INSERT INTO ethusd_minute (
	SELECT
	DATE_FORMAT(datetime_s, '%Y-%m-%d %H:%i'),
	SUBSTRING_INDEX( GROUP_CONCAT(CAST(price AS CHAR) ORDER BY datetime_s), ',', 1 ),
	MAX(price),
	MIN(price),
	SUBSTRING_INDEX( GROUP_CONCAT(CAST(price AS CHAR) ORDER BY datetime_s DESC), ',', 1 ),
    AVG(price),
	COUNT(*),
	SUM(price*volume)
	FROM ethusd_all_trades
	GROUP BY DATE_FORMAT(datetime_s, '%Y-%m-%d %H:%i')
	ORDER BY DATE_FORMAT(datetime_s, '%Y-%m-%d %H:%i') ASC
);

CREATE TABLE xbtusd_minute (
	datetime_min CHAR (16),
	price_open DOUBLE,
    price_high DOUBLE,
    price_low DOUBLE,
    price_close DOUBLE,
    price_avg DOUBLE,
    num_trades INT,
	volume_in_dollars DOUBLE
);

INSERT INTO xbtusd_minute(
	SELECT
	DATE_FORMAT(datetime_s, '%Y-%m-%d %H:%i'),
	SUBSTRING_INDEX( GROUP_CONCAT(CAST(price AS CHAR) ORDER BY datetime_s), ',', 1 ),
	MAX(price) AS price_high,
	MIN(price) AS price_low,
	SUBSTRING_INDEX( GROUP_CONCAT(CAST(price AS CHAR) ORDER BY datetime_s DESC), ',', 1 ),
    AVG(price),
	COUNT(*),
	SUM(price*volume)
	FROM xbtusd_all_trades
	GROUP BY DATE_FORMAT(datetime_s, '%Y-%m-%d %H:%i')
	ORDER BY DATE_FORMAT(datetime_s, '%Y-%m-%d %H:%i') ASC
);

CREATE TABLE ethusd_hour (
	datetime_hour CHAR (13),
	price_open DOUBLE,
    price_high DOUBLE,
    price_low DOUBLE,
    price_close DOUBLE,
    price_avg DOUBLE,
    num_trades INT,
	volume_in_dollars DOUBLE
);

INSERT INTO ethusd_hour (
	SELECT
	DATE_FORMAT(datetime_s, '%Y-%m-%d %H'),
	SUBSTRING_INDEX( GROUP_CONCAT(CAST(price AS CHAR) ORDER BY datetime_s), ',', 1 ),
	MAX(price),
	MIN(price),
	SUBSTRING_INDEX( GROUP_CONCAT(CAST(price AS CHAR) ORDER BY datetime_s DESC), ',', 1 ),
    AVG(price),
	COUNT(*),
	SUM(price*volume)
	FROM ethusd_all_trades
	GROUP BY DATE_FORMAT(datetime_s, '%Y-%m-%d %H')
	ORDER BY DATE_FORMAT(datetime_s, '%Y-%m-%d %H') ASC
);

CREATE TABLE xbtusd_hour (
	datetime_hour CHAR (13),
	price_open DOUBLE,
    price_high DOUBLE,
    price_low DOUBLE,
    price_close DOUBLE,
    price_avg DOUBLE,
    num_trades INT,
	volume_in_dollars DOUBLE
);

INSERT INTO xbtusd_hour (
	SELECT
	DATE_FORMAT(datetime_s, '%Y-%m-%d %H'),
	SUBSTRING_INDEX( GROUP_CONCAT(CAST(price AS CHAR) ORDER BY datetime_s), ',', 1 ),
	MAX(price),
	MIN(price),
	SUBSTRING_INDEX( GROUP_CONCAT(CAST(price AS CHAR) ORDER BY datetime_s DESC), ',', 1 ),
    AVG(price),
	COUNT(*),
	SUM(price*volume)
	FROM xbtusd_all_trades
	GROUP BY DATE_FORMAT(datetime_s, '%Y-%m-%d %H')
	ORDER BY DATE_FORMAT(datetime_s, '%Y-%m-%d %H') ASC
);

CREATE TABLE ethusd_month (
	month CHAR (7),
	price_open DOUBLE,
    price_high DOUBLE,
    price_low DOUBLE,
    price_close DOUBLE,
    price_avg DOUBLE,
    num_trades INT,
	volume_in_dollars DOUBLE
);

INSERT INTO ethusd_month (
	SELECT
	DATE_FORMAT(datetime_s, '%Y-%m'),
	SUBSTRING_INDEX( GROUP_CONCAT(CAST(price AS CHAR) ORDER BY datetime_s), ',', 1 ),
	MAX(price),
	MIN(price),
	SUBSTRING_INDEX( GROUP_CONCAT(CAST(price AS CHAR) ORDER BY datetime_s DESC), ',', 1 ),
    AVG(price),
	COUNT(*),
	SUM(price*volume)
	FROM ethusd_all_trades
	GROUP BY DATE_FORMAT(datetime_s, '%Y-%m')
	ORDER BY DATE_FORMAT(datetime_s, '%Y-%m') ASC
);

CREATE TABLE xbtusd_month (
	month CHAR (7),
	price_open DOUBLE,
    price_high DOUBLE,
    price_low DOUBLE,
    price_close DOUBLE,
    price_avg DOUBLE,
    num_trades INT,
	volume_in_dollars DOUBLE
);

 INSERT INTO xbtusd_month (
	SELECT
	DATE_FORMAT(datetime_s, '%Y-%m'),
	SUBSTRING_INDEX( GROUP_CONCAT(CAST(price AS CHAR) ORDER BY datetime_s), ',', 1 ),
	MAX(price),
	MIN(price),
	SUBSTRING_INDEX( GROUP_CONCAT(CAST(price AS CHAR) ORDER BY datetime_s DESC), ',', 1 ),
    AVG(price),
	COUNT(*),
	SUM(price*volume)
	FROM xbtusd_all_trades
	GROUP BY DATE_FORMAT(datetime_s, '%Y-%m')
	ORDER BY DATE_FORMAT(datetime_s, '%Y-%m') ASC
);

CREATE TABLE ethusd_year (
	year CHAR (4),
	price_open DOUBLE,
    price_high DOUBLE,
    price_low DOUBLE,
    price_close DOUBLE,
    price_avg DOUBLE,
    num_trades INT,
	volume_in_dollars DOUBLE
);

SET SESSION group_concat_max_len = 400000000;

INSERT INTO ethusd_year (
	SELECT
	DATE_FORMAT(datetime_s, '%Y'),
	SUBSTRING_INDEX( GROUP_CONCAT(CAST(price AS CHAR) ORDER BY datetime_s), ',', 1 ),
	MAX(price),
	MIN(price),
	SUBSTRING_INDEX( GROUP_CONCAT(CAST(price AS CHAR) ORDER BY datetime_s DESC), ',', 1 ),
    AVG(price),
	COUNT(*),
	SUM(price*volume)
	FROM ethusd_all_trades
	GROUP BY DATE_FORMAT(datetime_s, '%Y')
	ORDER BY DATE_FORMAT(datetime_s, '%Y') ASC
);

CREATE TABLE xbtusd_year (
	year CHAR (4),
	price_open DOUBLE,
    price_high DOUBLE,
    price_low DOUBLE,
    price_close DOUBLE,
    price_avg DOUBLE,
    num_trades INT,
	volume_in_dollars DOUBLE
);

INSERT INTO xbtusd_year (
	SELECT
	DATE_FORMAT(datetime_s, '%Y'),
	SUBSTRING_INDEX( GROUP_CONCAT(CAST(price AS CHAR) ORDER BY datetime_s), ',', 1 ),
	MAX(price),
	MIN(price),
	SUBSTRING_INDEX( GROUP_CONCAT(CAST(price AS CHAR) ORDER BY datetime_s DESC), ',', 1 ),
    AVG(price)g,
	COUNT(*),
	SUM(price*volume)
	FROM xbtusd_all_trades
	GROUP BY DATE_FORMAT(datetime_s, '%Y')
	ORDER BY DATE_FORMAT(datetime_s, '%Y') ASC
);