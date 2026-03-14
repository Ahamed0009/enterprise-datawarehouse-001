/*
===============================================================================
Stored Procedure: Load Curated Layer - crm_sales_details (Stage -> Curated)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to
    populate the 'curated.crm_sales_details' table from the 'stage.crm_sales_details' table.

    Actions Performed:
        - Validates the number of records to be loaded using COUNT.
        - Inserts transformed and cleansed data from Stage into Curated table.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL curated.load_crm_sales_details();
===============================================================================
*/

-- DROP PROCEDURE curated.load_crm_sales_details();

CREATE OR REPLACE PROCEDURE curated.load_crm_sales_details()
LANGUAGE plpgsql
AS $procedure$
DECLARE

    start_time TIMESTAMP;
    end_time TIMESTAMP;
    batch_start_time TIMESTAMP;
    batch_end_time TIMESTAMP;

    record_count BIGINT;
    error_region TEXT;

BEGIN

    batch_start_time := clock_timestamp();

    RAISE NOTICE '================================================';
    RAISE NOTICE 'Loading Table: curated.crm_sales_details';
    RAISE NOTICE '================================================';

    ------------------------------------------------
    -- REGION: COUNT VALIDATION
    ------------------------------------------------
    error_region := 'COUNT VALIDATION REGION';

    start_time := clock_timestamp();

    RAISE NOTICE '>> [START] COUNT VALIDATION REGION';

    SELECT COUNT(*)
    INTO record_count
    FROM (
        SELECT
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,

            CASE
                WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::TEXT) != 8
                THEN NULL
                ELSE TO_DATE(sls_order_dt::TEXT,'YYYYMMDD')
            END AS sls_order_dt,

            CASE
                WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::TEXT) != 8
                THEN NULL
                ELSE TO_DATE(sls_ship_dt::TEXT,'YYYYMMDD')
            END AS sls_ship_dt,

            CASE
                WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt::TEXT) != 8
                THEN NULL
                ELSE TO_DATE(sls_due_dt::TEXT,'YYYYMMDD')
            END AS sls_due_dt,

            CASE
                WHEN sls_sales IS NULL
                  OR sls_sales <= 0
                  OR sls_sales != sls_quantity * ABS(sls_price)
                THEN sls_quantity * ABS(sls_price)
                ELSE sls_sales
            END AS sls_sales,

            sls_quantity,

            CASE
                WHEN sls_price IS NULL OR sls_price <= 0
                THEN sls_sales / NULLIF(sls_quantity,0)
                ELSE sls_price
            END AS sls_price

        FROM stage.crm_sales_details
    ) src;

    RAISE NOTICE '>> Records to be inserted: %', record_count;

    end_time := clock_timestamp();

    RAISE NOTICE '>> [END] COUNT VALIDATION REGION';
    RAISE NOTICE '>> Duration: % seconds',
        EXTRACT(EPOCH FROM (end_time - start_time));
    RAISE NOTICE '>> -------------------------------------';


    ------------------------------------------------
    -- REGION: INSERT DATA
    ------------------------------------------------
    error_region := 'INSERT REGION';

    start_time := clock_timestamp();

    RAISE NOTICE '>> [START] INSERT REGION';

    TRUNCATE TABLE curated.crm_sales_details;

    INSERT INTO curated.crm_sales_details (
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price
    )
    SELECT
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,

        CASE
            WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::TEXT) != 8
            THEN NULL
            ELSE TO_DATE(sls_order_dt::TEXT,'YYYYMMDD')
        END,

        CASE
            WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::TEXT) != 8
            THEN NULL
            ELSE TO_DATE(sls_ship_dt::TEXT,'YYYYMMDD')
        END,

        CASE
            WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt::TEXT) != 8
            THEN NULL
            ELSE TO_DATE(sls_due_dt::TEXT,'YYYYMMDD')
        END,

        CASE
            WHEN sls_sales IS NULL
              OR sls_sales <= 0
              OR sls_sales != sls_quantity * ABS(sls_price)
            THEN sls_quantity * ABS(sls_price)
            ELSE sls_sales
        END,

        sls_quantity,

        CASE
            WHEN sls_price IS NULL OR sls_price <= 0
            THEN sls_sales / NULLIF(sls_quantity,0)
            ELSE sls_price
        END

    FROM stage.crm_sales_details;

    end_time := clock_timestamp();

    RAISE NOTICE '>> [END] INSERT REGION';
    RAISE NOTICE '>> Load Duration: % seconds',
        EXTRACT(EPOCH FROM (end_time - start_time));
    RAISE NOTICE '>> -------------------------------------';


    ------------------------------------------------
    -- BATCH COMPLETE
    ------------------------------------------------

    batch_end_time := clock_timestamp();

    RAISE NOTICE '================================================';
    RAISE NOTICE 'Load Completed: curated.crm_sales_details';
    RAISE NOTICE 'Total Duration: % seconds',
        EXTRACT(EPOCH FROM (batch_end_time - batch_start_time));
    RAISE NOTICE '================================================';


EXCEPTION
    WHEN OTHERS THEN

        RAISE NOTICE '================================================';
        RAISE NOTICE 'ERROR OCCURRED';
        RAISE NOTICE 'Error Region: %', error_region;
        RAISE NOTICE 'Error Message: %', SQLERRM;
        RAISE NOTICE '================================================';

END;
$procedure$;

