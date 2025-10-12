CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	PRINT 'Loading silver.crm_cust_info'
	PRINT '>> Truncating Table: silver.crm_cust_info';
	TRUNCATE TABLE silver.crm_cust_info;
	PRINT '>> Inserting Data Into: silver.crm_cust_info';
	WITH newTable AS (
	SELECT *
	FROM ( 
	SELECT *,
		ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last 
	FROM bronze.crm_cust_info ) AS T WHERE flag_last = 1 AND cst_id IS NOT NULL)
	INSERT INTO silver.crm_cust_info (
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date
	)

	SELECT cst_id,
	   cst_key, 
	   TRIM(cst_firstname) AS cst_firstname, 
	   TRIM(cst_lastname) AS cst_lastname, 
	   CASE 
			WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'MARRIED'
			WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'SINGLE'
			ELSE 'N/A' 
	   END AS cst_marital_status,
	   CASE
			WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'FEMALE'
			WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'MALE'
			ELSE 'N/A'
		END AS cst_gndr,
		cst_create_date
	FROM newTable

	PRINT '>> Truncating Table: silver.crm_prd_info';
	TRUNCATE TABLE silver.crm_prd_info;
	PRINT 'Loading silver.crm_prd_info'
	INSERT INTO silver.crm_prd_info
	SELECT 
		   prd_id,
		   REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
		   REPLACE(SUBSTRING(prd_key,7,LEN(prd_key)),'-','_') AS prd_key,
		   prd_nm,
		   ISNULL(prd_cost,0) AS prd_cost,
		   CASE 
			WHEN TRIM(prd_line) = 'M' THEN 'Master'
			WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Rocket'
			WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Sliver'
			WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Titan'
			ELSE 'N/A'
		   END AS prd_line,
		   CAST(prd_start_dt AS DATE),
		   CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_nm ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
	FROM bronze.crm_prd_info

	PRINT '>> Truncating Table: silver.crm_cust_info';
	TRUNCATE TABLE silver.crm_cust_info;
	PRINT 'Loading silver.crm_sales_Details'
	INSERT INTO silver.crm_sales_details (
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
		WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
		END AS sls_order_dt,
		CASE
		WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
		END AS sls_ship_dt,
		CASE
		WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
		END AS sls_due_dt,
		CASE 
			WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity*sls_price THEN sls_quantity*ABS(sls_price)
			ELSE sls_sales
		END AS sls_sales,
		sls_quantity,
		CASE 
			WHEN sls_price IS NULL OR sls_price <= 0 OR sls_price != sls_sales/sls_quantity THEN ABS(sls_sales)/sls_quantity
			ELSE sls_price
		END AS sls_price 
	FROM bronze.crm_sales_details


	PRINT '>> Truncating Table: silver.erp_CUST_AZ12';
	TRUNCATE TABLE silver.erp_CUST_AZ12
	PRINT 'Loading silver.erp_CUST_AZ12'
	INSERT INTO silver.erp_CUST_AZ12(
	CID,
	BDATE,
	GEN
	)
	SELECT
	CASE 
		WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID,4,LEN(CID))
		ELSE CID
	END AS CID,
	CASE 
		WHEN BDATE > GETDATE() THEN NULL
		ELSE CAST(BDATE AS DATE)
	END AS BDATE,
	CASE
		WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'female'
		WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'male'
		ELSE 'N/A'
	END AS GEN
	FROM bronze.erp_CUST_AZ12
	SELECT * FROM silver.erp_CUST_AZ12


	PRINT '>> Truncating Table: silver.erp_LOC_AZ12';
	TRUNCATE TABLE silver.erp_LOC_AZ12;
	PRINT 'Loading silver.erp_LOC_AZ12'
	INSERT INTO silver.erp_LOC_A101(CID,CNTRY)
	SELECT 
		REPLACE(CID,'-','') AS CID,
		CASE
			WHEN TRIM(CNTRY) IN ('US','USA') THEN 'United States'
			WHEN TRIM(CNTRY) = '' OR CNTRY IS NULL THEN  'N/A'
			WHEN TRIM(CNTRY) = 'DE' THEN 'Germany'
			ELSE TRIM(CNTRY)
		END AS CNTRY
	FROM bronze.erp_LOC_A101

	PRINT '>> Truncating Table: silver.erp_PX_CAT_G1V2';
	TRUNCATE TABLE silver.erp_PX_CAT_G1V2;
	PRINT 'Loading silver.erp_PX_CAT_G1V2'
	INSERT INTO silver.erp_PX_CAT_G1V2 (ID,CAT,SUBCAT,MAINTENANCE)
	SELECT 
		ID,
		CAT,
		SUBCAT,
		MAINTENANCE
	FROM bronze.erp_PX_CAT_G1V2 
END
