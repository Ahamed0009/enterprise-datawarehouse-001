/*
===============================================================================
DDL Script: Create Consumption Fact Table - fact_sales
===============================================================================
Purpose:
    This table stores sales transactions in the Consumption layer.

    It references dimension tables using surrogate keys and forms
    the center of the star schema for analytical queries.
===============================================================================
*/

DROP TABLE IF EXISTS consumption.fact_sales;

CREATE TABLE consumption.fact_sales (

    sales_key      BIGSERIAL PRIMARY KEY,   -- Surrogate key

    order_number   VARCHAR(50) NOT NULL,

    product_key    BIGINT NOT NULL,
    customer_key   BIGINT NOT NULL,

    order_date     DATE,
    shipping_date  DATE,
    due_date       DATE,

    sales_amount   INT,
    quantity       INT,
    price          INT,

    CONSTRAINT fk_fact_sales_product
        FOREIGN KEY (product_key)
        REFERENCES consumption.dim_products(product_key),

    CONSTRAINT fk_fact_sales_customer
        FOREIGN KEY (customer_key)
        REFERENCES consumption.dim_customers(customer_key)
);

-- Helpful indexes for analytics queries

CREATE INDEX idx_fact_sales_product
ON consumption.fact_sales(product_key);

CREATE INDEX idx_fact_sales_customer
ON consumption.fact_sales(customer_key);

CREATE INDEX idx_fact_sales_orderdate
ON consumption.fact_sales(order_date);

