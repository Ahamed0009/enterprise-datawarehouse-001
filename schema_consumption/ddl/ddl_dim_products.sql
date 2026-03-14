/*
===============================================================================
DDL Script: Create Consumption Dimension Table - dim_products
===============================================================================
Purpose:
    This table stores product dimension data in the Consumption layer.
    Surrogate key, primary key, and constraints are defined for star schema usage.
===============================================================================
*/

DROP TABLE IF EXISTS consumption.dim_products;

CREATE TABLE consumption.dim_products (
    product_key      BIGSERIAL PRIMARY KEY,   -- Surrogate key
    product_id       INT NOT NULL,
    product_number   VARCHAR(50) NOT NULL,
    product_name     VARCHAR(50),
    category_id      VARCHAR(50),
    category         VARCHAR(50),
    subcategory      VARCHAR(50),
    maintenance      VARCHAR(50),
    cost             INT,
    product_line     VARCHAR(50),
    start_date       DATE
);

-- Useful indexes
CREATE INDEX idx_dim_products_number
ON consumption.dim_products(product_number);

CREATE INDEX idx_dim_products_category
ON consumption.dim_products(category);
