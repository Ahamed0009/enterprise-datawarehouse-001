/*
===============================================================================
Stored Procedure: Load Consumption Layer - fact_sales
===============================================================================
Script Purpose:
    This stored procedure loads the fact_sales table in the Consumption layer
    by combining curated sales data with dimension surrogate keys.

Actions Performed:
    - Validates record count
    - Inserts transformed fact data
===============================================================================
*/

CREATE OR REPLACE PROCEDURE consumption.load_fact_sales()
LANGUAGE plpgsql
AS $procedure$

DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    batch_start_time TIMESTAMP;
    batch_end_time TIMESTAMP;

    record_count BIGINT;

BEGIN

    batch_start_time := clock_timestamp();

    RAISE NOTICE '================================================';
    RAISE NOTICE 'Loading Table: consumption.fact_sales';
    RAISE NOTICE '================================================';

    ------------------------------------------------
    -- COUNT VALIDATION REGION
    ------------------------------------------------

    start_time := clock_timestamp();

    RAISE NOTICE '>> [START] COUNT VALIDATION REGION';

    SELECT COUNT(*)
    INTO record_count
    FROM curated.crm_sales_details sd
    LEFT JOIN consumption.dim_products pr
        ON sd.sls_prd_key = pr.product_number
    LEFT JOIN consumption.dim_customers cu
        ON sd.sls_cust_id = cu.customer_id;

    RAISE NOTICE '>> Records to be inserted: %', record_count;

    RAISE NOTICE '>> [END] COUNT VALIDATION REGION';

    end_time := clock_timestamp();

    RAISE NOTICE '>> Duration: % seconds',
        EXTRACT(EPOCH FROM (end_time - start_time));

    RAISE NOTICE '>> -------------------------------------';

    ------------------------------------------------
    -- INSERT REGION
    ------------------------------------------------

    start_time := clock_timestamp();

    RAISE NOTICE '>> [START] INSERT REGION';

    TRUNCATE TABLE consumption.fact_sales;

    INSERT INTO consumption.fact_sales (

        order_number,
        product_key,
        customer_key,
        order_date,
        shipping_date,
        due_date,
        sales_amount,
        quantity,
        price
    )

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

    RAISE NOTICE '>> [END] INSERT REGION';

    end_time := clock_timestamp();

    RAISE NOTICE '>> Load Duration: % seconds',
        EXTRACT(EPOCH FROM (end_time - start_time));

    RAISE NOTICE '>> -------------------------------------';

    ------------------------------------------------
    -- BATCH END
    ------------------------------------------------

    batch_end_time := clock_timestamp();

    RAISE NOTICE '================================================';
    RAISE NOTICE 'Load Completed: consumption.fact_sales';
    RAISE NOTICE 'Total Duration: % seconds',
        EXTRACT(EPOCH FROM (batch_end_time - batch_start_time));
    RAISE NOTICE '================================================';

EXCEPTION

    WHEN OTHERS THEN

        RAISE NOTICE '========================================';
        RAISE NOTICE 'ERROR OCCURRED DURING fact_sales LOAD';
        RAISE NOTICE 'Error Message: %', SQLERRM;
        RAISE NOTICE '========================================';

END;
$procedure$;

