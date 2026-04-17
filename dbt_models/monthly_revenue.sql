-- models/monthly_revenue.sql
-- dbt model: Monthly revenue summary
-- Run with: dbt run --select monthly_revenue

{{ config(materialized='table') }}

SELECT
    year_month,
    SUM(revenue_usd)             AS total_revenue_usd,
    COUNT(order_id)              AS order_count,
    ROUND(AVG(revenue_usd), 2)   AS avg_order_value,
    COUNT(DISTINCT customer_id)  AS unique_customers
FROM {{ ref('sales_clean') }}
WHERE revenue_usd IS NOT NULL
GROUP BY year_month
ORDER BY year_month
