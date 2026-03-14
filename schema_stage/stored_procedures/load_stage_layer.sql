-- DROP PROCEDURE stage.load_stage_layer();

CREATE OR REPLACE PROCEDURE stage.load_stage_layer()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    batch_start_time TIMESTAMP;
    batch_end_time TIMESTAMP;
BEGIN

    batch_start_time := clock_timestamp();

    RAISE NOTICE '========================================';
    RAISE NOTICE 'Loading Stage Layer';
    RAISE NOTICE '========================================';

    ------------------------------------------------
    -- Loading CRM Tables
    ------------------------------------------------

    RAISE NOTICE '----------------------------------------';
    RAISE NOTICE 'Loading CRM Tables';
    RAISE NOTICE '----------------------------------------';

    -- crm_cust_info
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: stage.crm_cust_info';
    TRUNCATE TABLE stage.crm_cust_info;
    RAISE NOTICE '>> Inserting Data Into: stage.crm_cust_info';
    COPY stage.crm_cust_info
        FROM '/datasets/source_crm/cust_info.csv'
        DELIMITER ','
        CSV HEADER;
    end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
    RAISE NOTICE '>> -----------------------------';

    -- crm_prd_info
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: stage.crm_prd_info';
    TRUNCATE TABLE stage.crm_prd_info;
    RAISE NOTICE '>> Inserting Data Into: stage.crm_prd_info';
    COPY stage.crm_prd_info
        FROM '/datasets/source_crm/prd_info.csv'
        DELIMITER ','
        CSV HEADER;
    end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
    RAISE NOTICE '>> -----------------------------';

    -- crm_sales_details
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: stage.crm_sales_details';
    TRUNCATE TABLE stage.crm_sales_details;
    RAISE NOTICE '>> Inserting Data Into: stage.crm_sales_details';
    COPY stage.crm_sales_details
        FROM '/datasets/source_crm/sales_details.csv'
        DELIMITER ','
        CSV HEADER;
    end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
    RAISE NOTICE '>> -----------------------------';

    ------------------------------------------------
    -- Loading ERP Tables
    ------------------------------------------------

    RAISE NOTICE '----------------------------------------';
    RAISE NOTICE 'Loading ERP Tables';
    RAISE NOTICE '----------------------------------------';

    -- erp_loc_a101
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: stage.erp_loc_a101';
    TRUNCATE TABLE stage.erp_loc_a101;
    RAISE NOTICE '>> Inserting Data Into: stage.erp_loc_a101';
    COPY stage.erp_loc_a101
        FROM '/datasets/source_erp/LOC_A101.csv'
        DELIMITER ','
        CSV HEADER;
    end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
    RAISE NOTICE '>> -----------------------------';

    -- erp_cust_az12
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: stage.erp_cust_az12';
    TRUNCATE TABLE stage.erp_cust_az12;
    RAISE NOTICE '>> Inserting Data Into: stage.erp_cust_az12';
    COPY stage.erp_cust_az12
        FROM '/datasets/source_erp/CUST_AZ12.csv'
        DELIMITER ','
        CSV HEADER;
    end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
    RAISE NOTICE '>> -----------------------------';

    -- erp_px_cat_g1v2
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: stage.erp_px_cat_g1v2';
    TRUNCATE TABLE stage.erp_px_cat_g1v2;
    RAISE NOTICE '>> Inserting Data Into: stage.erp_px_cat_g1v2';
    COPY stage.erp_px_cat_g1v2
        FROM '/datasets/source_erp/PX_CAT_G1V2.csv'
        DELIMITER ','
        CSV HEADER;
    end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
    RAISE NOTICE '>> -----------------------------';

    ------------------------------------------------
    -- Batch End
    ------------------------------------------------

    batch_end_time := clock_timestamp();
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Loading Stage Layer Completed';
    RAISE NOTICE 'Total Load Duration: % seconds', EXTRACT(EPOCH FROM (batch_end_time - batch_start_time));
    RAISE NOTICE '========================================';

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE '========================================';
        RAISE NOTICE 'ERROR OCCURRED DURING LOADING STAGE LAYER';
        RAISE NOTICE 'Error Message: %', SQLERRM;
        RAISE NOTICE '========================================';

END;
$procedure$
;
