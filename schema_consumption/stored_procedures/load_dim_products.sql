/*
===============================================================================
Stored Procedure: Load Consumption Layer - dim_products
===============================================================================
Script Purpose:
    This stored procedure loads the dim_products table in the Consumption layer
    from curated tables.

    Actions Performed:
        - Validates record count
        - Inserts transformed product data
===============================================================================
*/

CREATE OR REPLACE PROCEDURE consumption.load_dim_products()
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
    RAISE NOTICE 'Loading Table: consumption.dim_products';
    RAISE NOTICE '================================================';

    ------------------------------------------------
    -- COUNT VALIDATION REGION
    ------------------------------------------------

    start_time := clock_timestamp();
    RAISE NOTICE '>> [START] COUNT VALIDATION REGION';

    SELECT COUNT(*)
    INTO record_count
    FROM curated.crm_prd_info pn
    LEFT JOIN curated.erp_px_cat_g1v2 pc
        ON pn.cat_id = pc.id
    WHERE pn.prd_end_dt IS NULL;

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

    TRUNCATE TABLE consumption.dim_products;

    INSERT INTO consumption.dim_products (
        product_id,
        product_number,
        product_name,
        category_id,
        category,
        subcategory,
        maintenance,
        cost,
        product_line,
        start_date
    )

    SELECT
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

    WHERE pn.prd_end_dt IS NULL;

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
    RAISE NOTICE 'Load Completed: consumption.dim_products';
    RAISE NOTICE 'Total Duration: % seconds',
        EXTRACT(EPOCH FROM (batch_end_time - batch_start_time));
    RAISE NOTICE '================================================';

EXCEPTION
    WHEN OTHERS THEN

        RAISE NOTICE '========================================';
        RAISE NOTICE 'ERROR OCCURRED DURING dim_products LOAD';
        RAISE NOTICE 'Error Message: %', SQLERRM;
        RAISE NOTICE '========================================';

END;
$procedure$;

