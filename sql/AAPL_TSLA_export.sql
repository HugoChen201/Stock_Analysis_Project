SELECT *
FROM stocks
WHERE ticker IN ('AAPL', 'TSLA')
ORDER BY ticker, Date;