-- models/top_products.sql
-- dbt model: Revenue by product category
-- Run with: dbt run --select top_products

{{ config(materialized='view') }}

SELECT
    product_category,
    COUNT(order_id)              AS total_orders,
    SUM(revenue_usd)             AS total_revenue_usd,
    ROUND(AVG(revenue_usd), 2)   AS avg_revenue,
    COUNT(DISTINCT customer_id)  AS unique_customers
FROM {{ ref('sales_clean') }}
WHERE product_category IS NOT NULL
GROUP BY product_category
ORDER BY total_revenue_usd DESC
