USE stock_project;

-- Verification Query
SELECT ticker,
       MIN(Date) AS start_date,
       MAX(Date) AS end_date,
       COUNT(*)   AS rows_per_ticker
FROM stocks
GROUP BY ticker
ORDER BY ticker;


