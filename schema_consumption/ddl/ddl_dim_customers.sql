/*
===============================================================================
DDL Script: Create Consumption Dimension Table - dim_customers
===============================================================================
Purpose:
    This table stores customer dimension data in the Consumption layer.
    Surrogate key, primary key, and constraints are defined for star schema usage.
===============================================================================
*/

DROP TABLE IF EXISTS consumption.dim_customers;

CREATE TABLE consumption.dim_customers (
    customer_key       BIGSERIAL PRIMARY KEY,  -- Surrogate key
    customer_id        INT NOT NULL,           -- Original customer ID
    customer_number    VARCHAR(50) NOT NULL,   -- Customer key from CRM
    first_name         VARCHAR(50),
    last_name          VARCHAR(50),
    country            VARCHAR(50),
    marital_status     VARCHAR(50),
    gender             VARCHAR(20),
    birthdate          DATE,
    create_date        DATE
);

-- Optional: Indexes for frequently queried columns
CREATE INDEX idx_dim_customers_number ON consumption.dim_customers(customer_number);
CREATE INDEX idx_dim_customers_country ON consumption.dim_customers(country);

