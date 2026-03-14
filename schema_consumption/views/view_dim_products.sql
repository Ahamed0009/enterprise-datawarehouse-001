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
-- Create Dimension: consumption.dim_products
-- =============================================================================
-- DROP VIEW IF EXISTS consumption.dim_products;

CREATE OR REPLACE VIEW consumption.view_dim_products AS
SELECT
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key, -- Surrogate key
    pn.prd_id       AS product_id,
    pn.prd_key      AS product_number,
    pn.prd_nm       AS product_name,
    pn.cat_id       AS category_id,
    pc.cat          AS category,
    pc.subcat       AS subcategory,
    pc.maintenance  AS maintenance,
    pn.prd_cost     AS cost,
    pn.prd_line     AS product_line,
    pn.prd_start_dt AS start_date
FROM curated.crm_prd_info pn
LEFT JOIN curated.erp_px_cat_g1v2 pc
    ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL; -- Filter out all historical data

