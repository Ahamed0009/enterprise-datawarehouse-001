-- DROP PROCEDURE curated.create_curated_tables();

CREATE OR REPLACE PROCEDURE curated.create_curated_tables()
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
    RAISE NOTICE 'Creating Curated Tables';
    RAISE NOTICE '========================================';

    ------------------------------------------------
    -- CRM Tables
    ------------------------------------------------

    start_time := clock_timestamp();
    RAISE NOTICE '>> Dropping and Creating Table: curated.crm_cust_info';

    DROP TABLE IF EXISTS curated.crm_cust_info;

    CREATE TABLE curated.crm_cust_info (
        cst_id             INT,
        cst_key            VARCHAR(50),
        cst_firstname      VARCHAR(50),
        cst_lastname       VARCHAR(50),
        cst_marital_status VARCHAR(50),
        cst_gndr           VARCHAR(50),
        cst_create_date    DATE,
        dwh_create_date    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    end_time := clock_timestamp();
    RAISE NOTICE '>> Table curated.crm_cust_info created in % seconds',
        EXTRACT(EPOCH FROM (end_time - start_time));
    RAISE NOTICE '>> -----------------------------';


    start_time := clock_timestamp();
    RAISE NOTICE '>> Dropping and Creating Table: curated.crm_prd_info';

    DROP TABLE IF EXISTS curated.crm_prd_info;

    CREATE TABLE curated.crm_prd_info (
        prd_id          INT,
        cat_id          VARCHAR(50),
        prd_key         VARCHAR(50),
        prd_nm          VARCHAR(50),
        prd_cost        INT,
        prd_line        VARCHAR(50),
        prd_start_dt    DATE,
        prd_end_dt      DATE,
        dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    end_time := clock_timestamp();
    RAISE NOTICE '>> Table curated.crm_prd_info created in % seconds',
        EXTRACT(EPOCH FROM (end_time - start_time));
    RAISE NOTICE '>> -----------------------------';


    start_time := clock_timestamp();
    RAISE NOTICE '>> Dropping and Creating Table: curated.crm_sales_details';

    DROP TABLE IF EXISTS curated.crm_sales_details;

    CREATE TABLE curated.crm_sales_details (
        sls_ord_num     VARCHAR(50),
        sls_prd_key     VARCHAR(50),
        sls_cust_id     INT,
        sls_order_dt    DATE,
        sls_ship_dt     DATE,
        sls_due_dt      DATE,
        sls_sales       INT,
        sls_quantity    INT,
        sls_price       INT,
        dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    end_time := clock_timestamp();
    RAISE NOTICE '>> Table curated.crm_sales_details created in % seconds',
        EXTRACT(EPOCH FROM (end_time - start_time));
    RAISE NOTICE '>> -----------------------------';


    ------------------------------------------------
    -- ERP Tables
    ------------------------------------------------

    start_time := clock_timestamp();
    RAISE NOTICE '>> Dropping and Creating Table: curated.erp_loc_a101';

    DROP TABLE IF EXISTS curated.erp_loc_a101;

    CREATE TABLE curated.erp_loc_a101 (
        cid             VARCHAR(50),
        cntry           VARCHAR(50),
        dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    end_time := clock_timestamp();
    RAISE NOTICE '>> Table curated.erp_loc_a101 created in % seconds',
        EXTRACT(EPOCH FROM (end_time - start_time));
    RAISE NOTICE '>> -----------------------------';


    start_time := clock_timestamp();
    RAISE NOTICE '>> Dropping and Creating Table: curated.erp_cust_az12';

    DROP TABLE IF EXISTS curated.erp_cust_az12;

    CREATE TABLE curated.erp_cust_az12 (
        cid             VARCHAR(50),
        bdate           DATE,
        gen             VARCHAR(50),
        dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    end_time := clock_timestamp();
    RAISE NOTICE '>> Table curated.erp_cust_az12 created in % seconds',
        EXTRACT(EPOCH FROM (end_time - start_time));
    RAISE NOTICE '>> -----------------------------';


    start_time := clock_timestamp();
    RAISE NOTICE '>> Dropping and Creating Table: curated.erp_px_cat_g1v2';

    DROP TABLE IF EXISTS curated.erp_px_cat_g1v2;

    CREATE TABLE curated.erp_px_cat_g1v2 (
        id              VARCHAR(50),
        cat             VARCHAR(50),
        subcat          VARCHAR(50),
        maintenance     VARCHAR(50),
        dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    end_time := clock_timestamp();
    RAISE NOTICE '>> Table curated.erp_px_cat_g1v2 created in % seconds',
        EXTRACT(EPOCH FROM (end_time - start_time));
    RAISE NOTICE '>> -----------------------------';


    ------------------------------------------------
    -- Batch End
    ------------------------------------------------

    batch_end_time := clock_timestamp();

    RAISE NOTICE '========================================';
    RAISE NOTICE 'Curated Tables Creation Completed';
    RAISE NOTICE 'Total Duration: % seconds',
        EXTRACT(EPOCH FROM (batch_end_time - batch_start_time));
    RAISE NOTICE '========================================';


EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE '========================================';
        RAISE NOTICE 'ERROR OCCURRED DURING CURATED TABLES CREATION';
        RAISE NOTICE 'Error Message: %', SQLERRM;
        RAISE NOTICE '========================================';

END;
$procedure$;

