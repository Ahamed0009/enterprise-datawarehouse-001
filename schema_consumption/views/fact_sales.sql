/*
===============================================================================
DDL Script: Create Consumption Views
===============================================================================
Script Purpose:
    This script creates views for the Consumption layer in the data warehouse. 
    The Consumption layer represents the final dimension and fact tables (Star Schema).

    Each view performs transformations and combines data from the Curated layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Fact Table: consumption.fact_sales
-- =============================================================================
-- DROP VIEW IF EXISTS consumption.fact_sales;

CREATE OR REPLACE VIEW consumption.fact_sales AS
SELECT
    sd.sls_ord_num  AS order_number,
    pr.product_key  AS product_key,
    cu.customer_key AS customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt  AS shipping_date,
    sd.sls_due_dt   AS due_date,
    sd.sls_sales    AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price    AS price
FROM curated.crm_sales_details sd
LEFT JOIN consumption.dim_products pr
    ON sd.sls_prd_key = pr.product_number
LEFT JOIN consumption.dim_customers cu
    ON sd.sls_cust_id = cu.customer_id;

