/*
--------------------------------------------------------------------------------------
    Description:
        This stored procedure loads raw data into the Bronze Layer of the data warehouse.
        It performs the following steps for each CRM and ERP table:
        
        - Truncates the existing table
        - Performs a BULK INSERT from the corresponding CSV file
        - Logs the time taken to load each table individually
        - Tracks and prints the total batch duration across all tables
        
        The procedure is wrapped in a TRY...CATCH block to handle and display any
        runtime errors during the load process.

    Tables Loaded:
        - bronze.crm_sales_details
        - bronze.crm_cust_info
        - bronze.crm_prd_info
        - bronze.erp_cust_az12
        - bronze.erp_loc_a101
        - bronze.erp_px_cat_g1v2

    Usage:
        Execute this procedure after preparing the corresponding source CSV files.
--------------------------------------------------------------------------------------
*/








CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME;
	DECLARE @batch_start_time DATETIME, @batch_end_time DATETIME;


	BEGIN TRY
	SET @batch_start_time = GETDATE();

		PRINT '========================'
		PRINT 'LOADING THE BRONZE LAYER'
		PRINT '========================'

		-- CRM TABLES
		PRINT '------------------------'
		PRINT 'LOADING CRM TABLES'

		-- crm_sales_details
		SET @start_time = GETDATE();
		PRINT '>>> TRUNCATING bronze.crm_sales_details'
		TRUNCATE TABLE bronze.crm_sales_details

		PRINT '>>> INSERTING DATA INTO bronze.crm_sales_details'
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\91628\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds'
		PRINT '---------------------------------------------------------------'

		-- crm_cust_info
		SET @start_time = GETDATE();
		PRINT '>>> TRUNCATING bronze.crm_cust_info'
		TRUNCATE TABLE bronze.crm_cust_info

		PRINT '>>> INSERTING DATA INTO bronze.crm_cust_info'
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\91628\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds'
		PRINT '---------------------------------------------------------------'

		-- crm_prd_info
		SET @start_time = GETDATE();
		PRINT '>>> TRUNCATING bronze.crm_prd_info'
		TRUNCATE TABLE bronze.crm_prd_info

		PRINT '>>> INSERTING DATA INTO bronze.crm_prd_info'
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\91628\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds'
		PRINT '---------------------------------------------------------------'

		-- ERP TABLES
		PRINT '------------------------'
		PRINT 'LOADING ERP TABLES'

		-- erp_cust_az12
		SET @start_time = GETDATE();
		PRINT '>>> TRUNCATING bronze.erp_cust_az12'
		TRUNCATE TABLE bronze.erp_cust_az12

		PRINT '>>> INSERTING DATA INTO bronze.erp_cust_az12'
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\91628\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds'
		PRINT '---------------------------------------------------------------'

		-- erp_loc_a101
		SET @start_time = GETDATE();
		PRINT '>>> TRUNCATING bronze.erp_loc_a101'
		TRUNCATE TABLE bronze.erp_loc_a101

		PRINT '>>> INSERTING DATA INTO bronze.erp_loc_a101'
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\91628\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds'
		PRINT '---------------------------------------------------------------'

		-- erp_px_cat_g1v2
		SET @start_time = GETDATE();
		PRINT '>>> TRUNCATING bronze.erp_px_cat_g1v2'
		TRUNCATE TABLE bronze.erp_px_cat_g1v2

		PRINT '>>> INSERTING DATA INTO bronze.erp_px_cat_g1v2'
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\91628\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds'
		PRINT '---------------------------------------------------------------'
	
		SET @batch_end_time = GETDATE();
		PRINT '========================'
		PRINT 'TOTAL BATCH LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS VARCHAR) + ' seconds'
		PRINT '========================'

	END TRY

	BEGIN CATCH
		PRINT '==========================='
		PRINT 'ERROR OCCURRED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message: ' + ERROR_MESSAGE();
		PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '==========================='
	END CATCH
END
