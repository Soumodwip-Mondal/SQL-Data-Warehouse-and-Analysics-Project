/*
=================================================================================================
Stored Procedure: This stored procedure will load the data into Bronze Layer.
Script purpose:
	This script will perform a full data load using a bulk insert operation in SQL. 
	This approach will import the entire CSV file into our data warehouse in a single operation.
==================================================================================================
*/

CREATE OR REPLACE PROCEDURE bronze_layer_load()
LANGUAGE plpgsql
AS $$
DECLARE
    -- File paths
    v_cust_info_path      TEXT := 'C:\Program Files\PostgreSQL\17\data\cust_info.csv';
    v_prd_info_path       TEXT := 'C:\Program Files\PostgreSQL\17\data\log\source_data\src_crm\prd_info.csv';
    v_sales_details_path  TEXT := 'C:\Program Files\PostgreSQL\17\data\log\source_data\src_crm\sales_details.csv';
    v_erp_cust_path       TEXT := 'C:\Program Files\PostgreSQL\17\data\log\source_data\src_erp\CUST_AZ12.csv';
    v_erp_loc_path        TEXT := 'C:\Program Files\PostgreSQL\17\data\log\source_data\src_erp\LOC_A101.csv';
    starttime             TIMESTAMP;
    endtime               TIMESTAMP;
    startbatchtime        TIMESTAMP;
    endbatchtime          TIMESTAMP;
BEGIN
    RAISE NOTICE '=========================================';
    RAISE NOTICE 'Loading the Bronze Layer';
    RAISE NOTICE '=========================================';

    -------------------------------------------------------------------------
    -- CRM Data Load
    -------------------------------------------------------------------------
    RAISE NOTICE '--------------------------------------';
    RAISE NOTICE 'Loading the CRM data';
    RAISE NOTICE '--------------------------------------';
    startbatchtime := clock_timestamp();

    -- crm_cust_info
    BEGIN
        starttime := clock_timestamp();
        TRUNCATE TABLE bronze_layer.crm_cust_info;
        EXECUTE format(
            'COPY bronze_layer.crm_cust_info FROM %L WITH (FORMAT CSV, HEADER)',
            v_cust_info_path
        );
        endtime := clock_timestamp();
        RAISE NOTICE 'crm_cust_info loaded successfully in % seconds',
            EXTRACT(EPOCH FROM (endtime - starttime));
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error loading crm_cust_info: %', SQLERRM;
    END;

    -- crm_prd_info
    BEGIN
        starttime := clock_timestamp();
        TRUNCATE TABLE bronze_layer.crm_prd_info;
        EXECUTE format(
            'COPY bronze_layer.crm_prd_info FROM %L WITH (FORMAT CSV, HEADER)',
            v_prd_info_path
        );
        endtime := clock_timestamp();
        RAISE NOTICE 'crm_prd_info loaded successfully in % seconds',
            EXTRACT(EPOCH FROM (endtime - starttime));
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error loading crm_prd_info: %', SQLERRM;
    END;

    -- crm_sales_details
    BEGIN
        starttime := clock_timestamp();
        TRUNCATE TABLE bronze_layer.crm_sales_details;
        EXECUTE format(
            'COPY bronze_layer.crm_sales_details FROM %L WITH (FORMAT CSV, HEADER)',
            v_sales_details_path
        );
        endtime := clock_timestamp();
        RAISE NOTICE 'crm_sales_details loaded successfully in % seconds',
            EXTRACT(EPOCH FROM (endtime - starttime));
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error loading crm_sales_details: %', SQLERRM;
    END;

    -------------------------------------------------------------------------
    -- ERP Data Load
    -------------------------------------------------------------------------
    RAISE NOTICE '--------------------------------------';
    RAISE NOTICE 'Loading the ERP data';
    RAISE NOTICE '--------------------------------------';

    -- erp_cust_az12
    BEGIN
        starttime := clock_timestamp();
        TRUNCATE TABLE bronze_layer.erp_cust_az12;
        EXECUTE format(
            'COPY bronze_layer.erp_cust_az12 FROM %L WITH (FORMAT CSV, HEADER)',
            v_erp_cust_path
        );
        endtime := clock_timestamp();
        RAISE NOTICE 'erp_cust_az12 loaded successfully in % seconds',
            EXTRACT(EPOCH FROM (endtime - starttime));
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error loading erp_cust_az12: %', SQLERRM;
    END;

    -- erp_loc_a101
    BEGIN
        starttime := clock_timestamp();
        TRUNCATE TABLE bronze_layer.erp_loc_a101;
        EXECUTE format(
            'COPY bronze_layer.erp_loc_a101 FROM %L WITH (FORMAT CSV, HEADER)',
            v_erp_loc_path
        );
        endtime := clock_timestamp();
        RAISE NOTICE 'erp_loc_a101 loaded successfully in % seconds',
            EXTRACT(EPOCH FROM (endtime - starttime));
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error loading erp_loc_a101: %', SQLERRM;
    END;

    -------------------------------------------------------------------------
    -- End of Load
    -------------------------------------------------------------------------
    endbatchtime := clock_timestamp();
    RAISE NOTICE '=========================================';
    RAISE NOTICE 'Bronze Layer loaded successfully in % seconds',
        EXTRACT(EPOCH FROM (endbatchtime - startbatchtime));
    RAISE NOTICE 'Bronze Layer Load Complete';
    RAISE NOTICE '=========================================';
END;
$$;

-- Run the procedure
CALL bronze_layer_load();
