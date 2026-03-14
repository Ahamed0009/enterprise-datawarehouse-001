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
-- Create Dimension: consumption.view_dim_customers
-- =============================================================================
-- DROP VIEW IF EXISTS consumption.view_dim_customers;

CREATE OR REPLACE VIEW consumption.view_dim_customers AS
SELECT
    ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key, -- Surrogate key
    ci.cst_id                          AS customer_id,
    ci.cst_key                         AS customer_number,
    ci.cst_firstname                   AS first_name,
    ci.cst_lastname                    AS last_name,
    la.cntry                           AS country,
    ci.cst_marital_status              AS marital_status,
    CASE 
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is the primary source for gender
        ELSE COALESCE(ca.gen, 'n/a')              -- Fallback to ERP data
    END                                AS gender,
    ca.bdate                           AS birthdate,
    ci.cst_create_date                 AS create_date
FROM curated.crm_cust_info ci
LEFT JOIN curated.erp_cust_az12 ca
    ON ci.cst_key = ca.cid
LEFT JOIN curated.erp_loc_a101 la
    ON ci.cst_key = la.cid;

