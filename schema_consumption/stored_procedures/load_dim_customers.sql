/*
===============================================================================
Stored Procedure: Load Consumption Layer - dim_customers
===============================================================================
Purpose:
    Populate dim_customers table from curated layer with transformations and validation.
===============================================================================
*/

CREATE OR REPLACE PROCEDURE consumption.load_dim_customers()
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

    RAISE NOTICE '========================================';
    RAISE NOTICE 'Loading Table: consumption.dim_customers';
    RAISE NOTICE '========================================';

    ------------------------------------------------
    -- Count Validation Region
    ------------------------------------------------
    RAISE NOTICE '>> [START] COUNT VALIDATION REGION';
    SELECT COUNT(*) INTO record_count
    FROM curated.crm_cust_info ci
    LEFT JOIN curated.erp_cust_az12 ca
        ON ci.cst_key = ca.cid
    LEFT JOIN curated.erp_loc_a101 la
        ON ci.cst_key = la.cid;
    RAISE NOTICE '>> Records to be inserted: %', record_count;
    RAISE NOTICE '>> [END] COUNT VALIDATION REGION';

    ------------------------------------------------
    -- Insert Region
    ------------------------------------------------
    RAISE NOTICE '>> [START] INSERT REGION';
    start_time := clock_timestamp();

    -- Optional: truncate before insert
    TRUNCATE TABLE consumption.dim_customers;

    INSERT INTO consumption.dim_customers (
        customer_id,
        customer_number,
        first_name,
        last_name,
        country,
        marital_status,
        gender,
        birthdate,
        create_date
    )
    SELECT
        ci.cst_id                          AS customer_id,
        ci.cst_key                         AS customer_number,
        ci.cst_firstname                   AS first_name,
        ci.cst_lastname                    AS last_name,
        la.cntry                           AS country,
        ci.cst_marital_status              AS marital_status,
        CASE 
            WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
            ELSE COALESCE(ca.gen, 'n/a')
        END                                AS gender,
        ca.bdate                           AS birthdate,
        ci.cst_create_date                 AS create_date
    FROM curated.crm_cust_info ci
    LEFT JOIN curated.erp_cust_az12 ca
        ON ci.cst_key = ca.cid
    LEFT JOIN curated.erp_loc_a101 la
        ON ci.cst_key = la.cid;

    end_time := clock_timestamp();
    RAISE NOTICE '>> [END] INSERT REGION';
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

    batch_end_time := clock_timestamp();
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Load Completed: consumption.dim_customers';
    RAISE NOTICE 'Total Duration: % seconds', EXTRACT(EPOCH FROM (batch_end_time - batch_start_time));
    RAISE NOTICE '========================================';

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE '========================================';
        RAISE NOTICE 'ERROR OCCURRED DURING consumption.dim_customers INSERT';
        RAISE NOTICE 'Error Message: %', SQLERRM;
        RAISE NOTICE '========================================';
END;
$procedure$;

