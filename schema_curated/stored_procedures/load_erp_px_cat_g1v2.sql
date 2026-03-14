/*
===============================================================================
Stored Procedure: Load Curated Layer - erp_px_cat_g1v2 (Stage -> Curated)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to
    populate the 'curated.erp_px_cat_g1v2' table from the 'stage.erp_px_cat_g1v2' table.

    Actions Performed:
        - Validates the number of records to be loaded using COUNT.
        - Inserts data from Stage into Curated table.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL curated.load_erp_px_cat_g1v2();
===============================================================================
*/

-- DROP PROCEDURE curated.load_erp_px_cat_g1v2();

CREATE OR REPLACE PROCEDURE curated.load_erp_px_cat_g1v2()
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
    RAISE NOTICE 'Loading Table: curated.erp_px_cat_g1v2';
    RAISE NOTICE '================================================';

    ------------------------------------------------
    -- REGION: COUNT VALIDATION
    ------------------------------------------------
    error_region := 'COUNT VALIDATION REGION';

    start_time := clock_timestamp();

    RAISE NOTICE '>> [START] COUNT VALIDATION REGION';

    SELECT COUNT(*)
    INTO record_count
    FROM stage.erp_px_cat_g1v2;

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

    TRUNCATE TABLE curated.erp_px_cat_g1v2;

    INSERT INTO curated.erp_px_cat_g1v2 (
        id,
        cat,
        subcat,
        maintenance
    )
    SELECT
        id,
        cat,
        subcat,
        maintenance
    FROM stage.erp_px_cat_g1v2;

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
    RAISE NOTICE 'Load Completed: curated.erp_px_cat_g1v2';
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

