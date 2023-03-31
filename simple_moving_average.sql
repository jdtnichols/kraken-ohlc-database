USE kraken;

SELECT
date,
price_open,
price_high,
price_low,
price_close,
AVG(price_close) OVER (ORDER BY date ROWS 20 PRECEDING) AS SMA_20,
AVG(price_close) OVER (ORDER BY date ROWS 50 PRECEDING) AS SMA_50,
AVG(price_close) OVER (ORDER BY date ROWS 20 PRECEDING) - AVG(price_close) OVER (ORDER BY date ROWS 50 PRECEDING) AS SMA_diff,
price_avg,
num_trades,
volume_in_dollars
FROM ethusd_day
ORDER BY date;