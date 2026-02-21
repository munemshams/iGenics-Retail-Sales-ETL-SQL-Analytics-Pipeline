-- ===========================================================
-- SQL Analytics for iGenics Weekly Sales Pipeline
-- Tables Used:
--    weekly_metrics_clean (loaded from ETL pipeline)
-- ===========================================================


-- -----------------------------------------------------------
-- 1. Total Revenue per Year
-- -----------------------------------------------------------
SELECT 
    year,
    SUM(value) AS total_revenue
FROM weekly_metrics_clean
WHERE LOWER(metric_name) = 'revenue'
GROUP BY year
ORDER BY year;


-- -----------------------------------------------------------
-- 2. Total Net Income per Year
-- -----------------------------------------------------------
SELECT 
    year,
    SUM(value) AS total_net_income
FROM weekly_metrics_clean
WHERE LOWER(metric) = 'total net income'
GROUP BY year
ORDER BY year;


-- -----------------------------------------------------------
-- 3. Most Profitable Week (Highest Net Income)
-- -----------------------------------------------------------
SELECT 
    year,
    week,
    SUM(value) AS net_income
FROM weekly_metrics_clean
WHERE LOWER(metric) = 'total net income'
GROUP BY year, week
ORDER BY net_income DESC
LIMIT 1;


-- -----------------------------------------------------------
-- 4. Average Weekly Revenue per Year
-- -----------------------------------------------------------
SELECT
    year,
    AVG(value) AS avg_weekly_revenue
FROM weekly_metrics_clean
WHERE LOWER(metric_name) = 'revenue'
GROUP BY year
ORDER BY year;


-- -----------------------------------------------------------
-- 5. Weekly Revenue Trend (Used for Visualization)
-- -----------------------------------------------------------
SELECT
    year,
    week,
    SUM(value) AS weekly_revenue
FROM weekly_metrics_clean
WHERE LOWER(metric_name) = 'revenue'
GROUP BY year, week
ORDER BY year, week;