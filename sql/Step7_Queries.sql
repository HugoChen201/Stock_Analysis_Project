-- Step 7: SQL Query Analysis List of Queries

-- Query 1: Average Return and Volatility Per Ticker
SELECT ticker, 
	AVG(daily_return) AS avg_daily_return, 
    STDDEV_SAMP(daily_return) AS daily_return_vol
FROM stock_project.stocks
GROUP BY ticker
ORDER BY ticker;

-- Query 2: Monthly Returns Per Ticker
SELECT ticker, 
	YEAR(date) as yr, 
    MONTH(date) as mo, 
    AVG(daily_return) AS monthly_return
FROM stock_project.stocks
GROUP BY ticker, YEAR(date), MONTH(date)
ORDER BY ticker, yr, mo;

-- Query 3: Most Volatile Days
SELECT ticker, date, daily_return, 
	ABS(daily_return) AS abs_return
FROM stock_project.stocks
ORDER BY abs_return DESC
LIMIT 20;

-- Query 4: Total 5 Year Return By Ticker
SELECT ticker, 
	SUM(daily_return) * 100 AS total_5y_return_percent
FROM stock_project.stocks
GROUP BY ticker
ORDER BY ticker;

-- Query 5: CAGR (Compound Annual Growth Rate) By Ticker
SELECT ticker, 
	POWER(1 + SUM(daily_return), 1/5) - 1 AS cagr_5y
FROM stock_project.stocks
GROUP BY ticker
ORDER BY ticker;

-- Query 6: Annual Return By Ticker
SELECT ticker, 
	YEAR(date) as yr, SUM(daily_return) as annual_return
FROM stock_project.stocks
GROUP BY ticker, YEAR(date)
ORDER BY ticker, yr;

-- Query 7: Risk/Return Summary
SELECT ticker,
    AVG(daily_return)                         AS avg_daily_return,
    SUM(daily_return)                         AS total_return_5y,
    POWER(1 + SUM(daily_return), 1/5) - 1     AS cagr_5y,
    STDDEV_SAMP(daily_return)                 AS daily_vol
FROM stock_project.stocks
GROUP BY ticker
ORDER BY ticker;

-- Query 8: Sharpe Ratio By Ticker
SELECT ticker, 
	AVG(daily_return)                      AS avg_daily_return,
    STDDEV_SAMP(daily_return)              AS daily_vol,
    AVG(daily_return) / STDDEV_SAMP(daily_return) AS sharpe_ratio
FROM stock_project.stocks
GROUP BY ticker
ORDER BY sharpe_ratio DESC; 

-- Query 9: Trend Up and Down Returns
SELECT ticker, trend_20_50,
    COUNT(*)               AS days_in_state,
    AVG(daily_return)      AS avg_return_in_state
FROM stock_project.stocks
WHERE trend_20_50 IS NOT NULL
GROUP BY ticker, trend_20_50
ORDER BY ticker, trend_20_50;

-- Query 10: Up-Trend Hitrate
SELECT ticker,
    COUNT(*) AS uptrend_days,
    SUM(CASE WHEN daily_return > 0 THEN 1 ELSE 0 END) AS positive_days,
    SUM(CASE WHEN daily_return > 0 THEN 1 ELSE 0 END) / COUNT(*) AS hit_rate
FROM stock_project.stocks
WHERE trend_20_50 = 'Uptrend'
GROUP BY ticker
ORDER BY ticker;






