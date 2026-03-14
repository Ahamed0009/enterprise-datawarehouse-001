/*
===============================================================================
Stored Procedure: Load Curated Layer - crm_cust_info (Stage -> Curated)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to
    populate the 'curated.crm_cust_info' table from the 'stage.crm_cust_info' table.

    Actions Performed:
        - Validates the number of records to be loaded using COUNT.
        - Inserts transformed and cleansed data from Stage into Curated table.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL curated.load_crm_cust_info();
===============================================================================
*/

-- DROP PROCEDURE curated.load_crm_cust_info();

CREATE OR REPLACE PROCEDURE curated.load_crm_cust_info()
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
    RAISE NOTICE 'Loading Table: curated.crm_cust_info';
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
            cst_id,
            cst_key,
            TRIM(cst_firstname) AS cst_firstname,
            TRIM(cst_lastname) AS cst_lastname,
            CASE
                WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
                WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
                ELSE 'n/a'
            END AS cst_marital_status,
            CASE
                WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                ELSE 'n/a'
            END AS cst_gndr,
            cst_create_date
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY cst_id
                    ORDER BY cst_create_date DESC
                ) AS flag_last
            FROM stage.crm_cust_info
            WHERE cst_id IS NOT NULL
        ) t
        WHERE flag_last = 1
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

    TRUNCATE TABLE curated.crm_cust_info;

    INSERT INTO curated.crm_cust_info (
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gndr,
        cst_create_date
    )
    SELECT
        cst_id,
        cst_key,
        TRIM(cst_firstname),
        TRIM(cst_lastname),
        CASE
            WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
            ELSE 'n/a'
        END,
        CASE
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            ELSE 'n/a'
        END,
        cst_create_date
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY cst_id
                ORDER BY cst_create_date DESC
            ) AS flag_last
        FROM stage.crm_cust_info
        WHERE cst_id IS NOT NULL
    ) t
    WHERE flag_last = 1;

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
    RAISE NOTICE 'Load Completed: curated.crm_cust_info';
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

