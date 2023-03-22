CREATE SCHEMA kraken;

USE kraken;

CREATE TABLE ethusd_raw (
	trade_id INT  NOT NULL AUTO_INCREMENT PRIMARY KEY,
    trade_timestamp INT,
    price FLOAT,
   volume FLOAT
);

CREATE TABLE xbtusd_raw (
	trade_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
	trade_timestamp INT,
	price FLOAT,
	volume FLOAT
);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/ethusd_id.csv' INTO TABLE ethusd_raw
	COLUMNS TERMINATED BY ','
    LINES TERMINATED BY '\r\n';

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/xbtusd_id.csv' INTO TABLE xbtusd_raw
	COLUMNS TERMINATED BY ','
    LINES TERMINATED BY '\r\n';
    
CREATE TABLE ethusd_all_trades AS (
	SELECT
	trade_id,
    trade_timestamp,
    FROM_UNIXTIME(trade_timestamp, '%Y-%m-%d %H:%i:%s') AS datetime_s,
	price,
	volume
	FROM ethusd_raw
);

CREATE TABLE xbtusd_all_trades AS (
	SELECT
	trade_id,
    trade_timestamp,
    FROM_UNIXTIME(trade_timestamp, '%Y-%m-%d %H:%i:%s') AS datetime_s,
	price,
	volume
	FROM xbtusd_raw
);

SET SESSION group_concat_max_len = 40000000;

CREATE TABLE ethusd_day AS (
	SELECT
	DATE_FORMAT(datetime_s, '%Y-%m-%d') AS date,
	SUBSTRING_INDEX( GROUP_CONCAT(CAST(price AS CHAR) ORDER BY datetime_s), ',', 1 ) AS price_open,
	MAX(price) AS price_high,
	MIN(price) AS price_low,
	SUBSTRING_INDEX( GROUP_CONCAT(CAST(price AS CHAR) ORDER BY datetime_s DESC), ',', 1 ) AS price_close,
	COUNT(*) AS num_trades,
	SUM(price*volume) AS volume_in_dollars,
	(SUM(price*volume))/(SUBSTRING_INDEX( GROUP_CONCAT(CAST(price AS CHAR) ORDER BY datetime_s DESC), ',', 1 )) AS price_normalized_volume,
	MAX(price) - MIN(price) AS range_hilow,
	(MAX(price) - MIN(price))/(SUBSTRING_INDEX( GROUP_CONCAT(CAST(price AS CHAR) ORDER BY datetime_s DESC), ',', 1 )) AS price_normalized_range
	FROM ethusd_all_trades
	GROUP BY date
	ORDER BY date ASC
);

CREATE TABLE xbtusd_day AS (
	SELECT
	DATE_FORMAT(datetime_s, '%Y-%m-%d') AS date,
	SUBSTRING_INDEX( GROUP_CONCAT(CAST(price AS CHAR) ORDER BY datetime_s), ',', 1 ) AS price_open,
	MAX(price) AS price_high,
	MIN(price) AS price_low,
	SUBSTRING_INDEX( GROUP_CONCAT(CAST(price AS CHAR) ORDER BY datetime_s DESC), ',', 1 ) AS price_close,
	COUNT(*) AS num_trades,
	SUM(price*volume) AS volume_in_dollars,
	(SUM(price*volume))/(SUBSTRING_INDEX( GROUP_CONCAT(CAST(price AS CHAR) ORDER BY datetime_s DESC), ',', 1 )) AS price_normalized_volume,
	MAX(price) - MIN(price) AS range_hilow,
	(MAX(price) - MIN(price))/(SUBSTRING_INDEX( GROUP_CONCAT(CAST(price AS CHAR) ORDER BY datetime_s DESC), ',', 1 )) AS price_normalized_range
	FROM xbtusd_all_trades
	GROUP BY date
	ORDER BY date ASC
);

CREATE TABLE ethusd_minute AS (
	SELECT
	DATE_FORMAT(datetime_s, '%Y-%m-%d %H:%i') AS datetime_min,
	SUBSTRING_INDEX( GROUP_CONCAT(CAST(price AS CHAR) ORDER BY datetime_s), ',', 1 ) AS price_open,
	MAX(price) AS price_high,
	MIN(price) AS price_low,
	SUBSTRING_INDEX( GROUP_CONCAT(CAST(price AS CHAR) ORDER BY datetime_s DESC), ',', 1 ) AS price_close,
	COUNT(*) AS num_trades,
	SUM(price*volume) AS volume_in_dollars,
	(SUM(price*volume))/(SUBSTRING_INDEX( GROUP_CONCAT(CAST(price AS CHAR) ORDER BY datetime_s DESC), ',', 1 )) AS price_normalized_volume,
	MAX(price) - MIN(price) AS range_hilow,
	(MAX(price) - MIN(price))/(SUBSTRING_INDEX( GROUP_CONCAT(CAST(price AS CHAR) ORDER BY datetime_s DESC), ',', 1 )) AS price_normalized_range
	FROM ethusd_all_trades
	GROUP BY datetime_min
	ORDER BY datetime_min ASC
);

CREATE TABLE xbtusd_minute AS (
	SELECT
	DATE_FORMAT(datetime_s, '%Y-%m-%d %H:%i') AS datetime_min,
	SUBSTRING_INDEX( GROUP_CONCAT(CAST(price AS CHAR) ORDER BY datetime_s), ',', 1 ) AS price_open,
	MAX(price) AS price_high,
	MIN(price) AS price_low,
	SUBSTRING_INDEX( GROUP_CONCAT(CAST(price AS CHAR) ORDER BY datetime_s DESC), ',', 1 ) AS price_close,
	COUNT(*) AS num_trades,
	SUM(price*volume) AS volume_in_dollars,
	(SUM(price*volume))/(SUBSTRING_INDEX( GROUP_CONCAT(CAST(price AS CHAR) ORDER BY datetime_s DESC), ',', 1 )) AS price_normalized_volume,
	MAX(price) - MIN(price) AS range_hilow,
	(MAX(price) - MIN(price))/(SUBSTRING_INDEX( GROUP_CONCAT(CAST(price AS CHAR) ORDER BY datetime_s DESC), ',', 1 )) AS price_normalized_range
	FROM xbtusd_all_trades
	GROUP BY datetime_min
	ORDER BY datetime_min ASC
);

CREATE TABLE ethusd_hour AS (
	SELECT
	DATE_FORMAT(datetime_s, '%Y-%m-%d %H') AS datetime_hour,
	SUBSTRING_INDEX( GROUP_CONCAT(CAST(price AS CHAR) ORDER BY datetime_s), ',', 1 ) AS price_open,
	MAX(price) AS price_high,
	MIN(price) AS price_low,
	SUBSTRING_INDEX( GROUP_CONCAT(CAST(price AS CHAR) ORDER BY datetime_s DESC), ',', 1 ) AS price_close,
	COUNT(*) AS num_trades,
	SUM(price*volume) AS volume_in_dollars,
	(SUM(price*volume))/(SUBSTRING_INDEX( GROUP_CONCAT(CAST(price AS CHAR) ORDER BY datetime_s DESC), ',', 1 )) AS price_normalized_volume,
	MAX(price) - MIN(price) AS range_hilow,
	(MAX(price) - MIN(price))/(SUBSTRING_INDEX( GROUP_CONCAT(CAST(price AS CHAR) ORDER BY datetime_s DESC), ',', 1 )) AS price_normalized_range
	FROM ethusd_all_trades
	GROUP BY datetime_hour
	ORDER BY datetime_hour ASC
);

CREATE TABLE xbtusd_hour AS (
	SELECT
	DATE_FORMAT(datetime_s, '%Y-%m-%d %H') AS datetime_hour,
	SUBSTRING_INDEX( GROUP_CONCAT(CAST(price AS CHAR) ORDER BY datetime_s), ',', 1 ) AS price_open,
	MAX(price) AS price_high,
	MIN(price) AS price_low,
	SUBSTRING_INDEX( GROUP_CONCAT(CAST(price AS CHAR) ORDER BY datetime_s DESC), ',', 1 ) AS price_close,
	COUNT(*) AS num_trades,
	SUM(price*volume) AS volume_in_dollars,
	(SUM(price*volume))/(SUBSTRING_INDEX( GROUP_CONCAT(CAST(price AS CHAR) ORDER BY datetime_s DESC), ',', 1 )) AS price_normalized_volume,
	MAX(price) - MIN(price) AS range_hilow,
	(MAX(price) - MIN(price))/(SUBSTRING_INDEX( GROUP_CONCAT(CAST(price AS CHAR) ORDER BY datetime_s DESC), ',', 1 )) AS price_normalized_range
	FROM xbtusd_all_trades
	GROUP BY datetime_hour
	ORDER BY datetime_hour ASC
);

CREATE TABLE ethusd_month AS (
	SELECT
	DATE_FORMAT(datetime_s, '%Y-%m') AS month,
	SUBSTRING_INDEX( GROUP_CONCAT(CAST(price AS CHAR) ORDER BY datetime_s), ',', 1 ) AS price_open,
	MAX(price) AS price_high,
	MIN(price) AS price_low,
	SUBSTRING_INDEX( GROUP_CONCAT(CAST(price AS CHAR) ORDER BY datetime_s DESC), ',', 1 ) AS price_close,
	COUNT(*) AS num_trades,
	SUM(price*volume) AS volume_in_dollars,
	(SUM(price*volume))/(SUBSTRING_INDEX( GROUP_CONCAT(CAST(price AS CHAR) ORDER BY datetime_s DESC), ',', 1 )) AS price_normalized_volume,
	MAX(price) - MIN(price) AS range_hilow,
	(MAX(price) - MIN(price))/(SUBSTRING_INDEX( GROUP_CONCAT(CAST(price AS CHAR) ORDER BY datetime_s DESC), ',', 1 )) AS price_normalized_range
	FROM ethusd_all_trades
	GROUP BY month
	ORDER BY month ASC
);

CREATE TABLE xbtusd_month AS (
	SELECT
	DATE_FORMAT(datetime_s, '%Y-%m') AS month,
	SUBSTRING_INDEX( GROUP_CONCAT(CAST(price AS CHAR) ORDER BY datetime_s), ',', 1 ) AS price_open,
	MAX(price) AS price_high,
	MIN(price) AS price_low,
	SUBSTRING_INDEX( GROUP_CONCAT(CAST(price AS CHAR) ORDER BY datetime_s DESC), ',', 1 ) AS price_close,
	COUNT(*) AS num_trades,
	SUM(price*volume) AS volume_in_dollars,
	(SUM(price*volume))/(SUBSTRING_INDEX( GROUP_CONCAT(CAST(price AS CHAR) ORDER BY datetime_s DESC), ',', 1 )) AS price_normalized_volume,
	MAX(price) - MIN(price) AS range_hilow,
	(MAX(price) - MIN(price))/(SUBSTRING_INDEX( GROUP_CONCAT(CAST(price AS CHAR) ORDER BY datetime_s DESC), ',', 1 )) AS price_normalized_range
	FROM xbtusd_all_trades
	GROUP BY month
	ORDER BY month ASC
);

SET SESSION group_concat_max_len = 400000000;

CREATE TABLE ethusd_year AS (
	SELECT
	DATE_FORMAT(datetime_s, '%Y') AS year,
	SUBSTRING_INDEX( GROUP_CONCAT(CAST(price AS CHAR) ORDER BY datetime_s), ',', 1 ) AS price_open,
	MAX(price) AS price_high,
	MIN(price) AS price_low,
	SUBSTRING_INDEX( GROUP_CONCAT(CAST(price AS CHAR) ORDER BY datetime_s DESC), ',', 1 ) AS price_close,
	COUNT(*) AS num_trades,
	SUM(price*volume) AS volume_in_dollars,
	(SUM(price*volume))/(SUBSTRING_INDEX( GROUP_CONCAT(CAST(price AS CHAR) ORDER BY datetime_s DESC), ',', 1 )) AS price_normalized_volume,
	MAX(price) - MIN(price) AS range_hilow,
	(MAX(price) - MIN(price))/(SUBSTRING_INDEX( GROUP_CONCAT(CAST(price AS CHAR) ORDER BY datetime_s DESC), ',', 1 )) AS price_normalized_range
	FROM ethusd_all_trades
	GROUP BY year
	ORDER BY year ASC
);

CREATE TABLE xbtusd_year AS (
	SELECT
	DATE_FORMAT(datetime_s, '%Y') AS year,
	SUBSTRING_INDEX( GROUP_CONCAT(CAST(price AS CHAR) ORDER BY datetime_s), ',', 1 ) AS price_open,
	MAX(price) AS price_high,
	MIN(price) AS price_low,
	SUBSTRING_INDEX( GROUP_CONCAT(CAST(price AS CHAR) ORDER BY datetime_s DESC), ',', 1 ) AS price_close,
	COUNT(*) AS num_trades,
	SUM(price*volume) AS volume_in_dollars,
	(SUM(price*volume))/(SUBSTRING_INDEX( GROUP_CONCAT(CAST(price AS CHAR) ORDER BY datetime_s DESC), ',', 1 )) AS price_normalized_volume,
	MAX(price) - MIN(price) AS range_hilow,
	(MAX(price) - MIN(price))/(SUBSTRING_INDEX( GROUP_CONCAT(CAST(price AS CHAR) ORDER BY datetime_s DESC), ',', 1 )) AS price_normalized_range
	FROM xbtusd_all_trades
	GROUP BY year
	ORDER BY year ASC
);