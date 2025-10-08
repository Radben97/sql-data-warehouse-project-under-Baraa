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
