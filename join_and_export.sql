USE kraken;

SELECT 'date', 'eth_price_open', 'eth_price_high', 'eth_price_low', 'eth_price_close', 'eth_price_avg', 'eth_num_trades', 'eth_volume_in_dollars', 'xbt_price_open', 'xbt_price_high', 'xbt_price_low', 'xbt_price_close', 'xbt_price_avg', 'xbt_num_trades', 'xbt_volume_in_dollars'
UNION
SELECT
ethusd_day.date,
ethusd_day.price_open,
ethusd_day.price_high,
ethusd_day.price_low,
ethusd_day.price_close,
ethusd_day.price_avg,
ethusd_day.num_trades,
ethusd_day.volume_in_dollars,
xbtusd_day.price_open,
xbtusd_day.price_high,
xbtusd_day.price_low,
xbtusd_day.price_close,
xbtusd_day.price_avg,
xbtusd_day.num_trades,
xbtusd_day.volume_in_dollars
FROM ethusd_day
INNER JOIN xbtusd_day ON ethusd_day.date = xbtusd_day.date
INTO OUTFILE '[OUTFILE LOCATION]/xbtusd_ethusd_day_innerjoin.csv'
COLUMNS TERMINATED BY ','
LINES TERMINATED BY '\r\n';