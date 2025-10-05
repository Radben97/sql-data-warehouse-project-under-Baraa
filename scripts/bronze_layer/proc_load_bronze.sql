CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME,@end_time DATETIME;
	SET @start_time = GETDATE();
	BEGIN TRY
		PRINT '=========================================='
		PRINT 'Loading Bronze Layer...'
		PRINT '=========================================='
		PRINT '------------------------------------------'
		PRINT 'Loading CRM Tables'
		PRINT '------------------------------------------'
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_cust_info
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Personal\Learning\sqlDataWarehouse\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration For cust_info.csv : ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR(50)) + ' seconds';
		PRINT '------------------------------------------'

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_prd_info
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Personal\Learning\sqlDataWarehouse\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration For prd_info.csv : ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR(50)) + ' seconds';
		PRINT '------------------------------------------'

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_sales_details
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Personal\Learning\sqlDataWarehouse\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration For sales_details.csv : ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR(50)) + ' seconds';
		PRINT '------------------------------------------'

		PRINT '------------------------------------------'
		PRINT 'Loading ERP Tables'
		PRINT '------------------------------------------'

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_CUST_AZ12
		BULK INSERT bronze.erp_CUST_AZ12
		FROM 'C:\Personal\Learning\sqlDataWarehouse\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration For CUST_AZ12.csv : ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR(50)) + ' seconds';
		PRINT '------------------------------------------'

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_LOC_A101
		BULK INSERT bronze.erp_LOC_A101
		FROM 'C:\Personal\Learning\sqlDataWarehouse\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration For LOC_A101.csv : ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR(50)) + ' seconds';
		PRINT '------------------------------------------'

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_PX_CAT_G1V2
		BULK INSERT bronze.erp_PX_CAT_G1V2
		FROM 'C:\Personal\Learning\sqlDataWarehouse\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration For PX_CAT_G1V2.csv : ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR(50)) + ' seconds';
		PRINT '------------------------------------------'
	END TRY
	BEGIN CATCH
		PRINT '==========================================';
		PRINT 'Error While Loading Bronze Layer';
		PRINT CAST(ERROR_MESSAGE() AS NVARCHAR(50));
		PRINT CAST(ERROR_STATE() AS NVARCHAR(50));
		PRINT CAST(ERROR_NUMBER() AS NVARCHAR(50));
		PRINT '==========================================';
	END CATCH
	SET @end_time = GETDATE();
	PRINT '>> Load Duration Of Bronze Layer : ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR(50)) + ' seconds';
	PRINT '------------------------------------------'
END

DROP PROCEDURE bronze.load_bronze
