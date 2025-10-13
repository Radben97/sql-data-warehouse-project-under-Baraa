IF OBJECT_ID('gold.dim_customers','V') IS NOT NULL
	DROP VIEW gold.dim_customers
GO
CREATE VIEW gold.dim_customers AS
SELECT 
	ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
	ca.cst_id AS customer_id,
	ca.cst_key AS customer_number,
	ca.cst_firstname AS first_name,
	ca.cst_lastname AS last_name,
	la.CNTRY AS country,
	ca.cst_marital_status AS marital_status,
	CASE
		WHEN cst_gndr != 'N/A' THEN cst_gndr
		ELSE UPPER(COALESCE(cu.GEN, 'N/A'))
	END AS gender,
	ca.cst_create_date AS create_date,
	cu.BDATE AS birthdate
FROM silver.crm_cust_info AS ca
LEFT JOIN silver.erp_LOC_A101 AS la
ON ca.cst_key = la.CID
LEFT JOIN silver.erp_CUST_AZ12 AS cu
ON ca.cst_key = cu.CID

IF OBJECT_ID('gold.dim_products','V') IS NOT NULL
	DROP VIEW gold.dim_products
GO
CREATE VIEW gold.dim_products AS
SELECT 
	ROW_NUMBER() OVER (ORDER BY prd_start_dt,prd_key) AS product_key,
	prd_id AS product_id,
	prd_key AS product_number,
	prd_nm AS product_name,
	REPLACE(cat_id,'_','-') AS category_id,
	px.CAT AS category,
	px.SUBCAT AS sub_category,
	px.MAINTENANCE AS maintenance,
	prd_cost AS cost,
	prd_line AS product_line,
	prd_start_dt AS 'start_date'
FROM silver.crm_prd_info as pa
LEFT JOIN silver.erp_PX_CAT_G1V2 AS px
ON px.ID = pa.prd_key
WHERE prd_end_dt IS NULL

IF OBJECT_ID('gold.fact_sales','V') IS NOT NULL
	DROP VIEW gold.fact_sales
GO
CREATE VIEW gold.fact_sales AS
SELECT 
	sls_ord_num AS order_number,
	dp.product_key,
	dc.customer_key,
	sls_order_dt AS order_Date,
	sls_ship_dt AS shipping_date,
	sls_due_dt AS due_date,
	sls_sales AS sales_amount,
	sls_quantity AS quantity,
	sls_price AS price
FROM silver.crm_sales_details AS sd
LEFT JOIN  gold.dim_products AS dp
ON sd.sls_prd_key = dp.category_id
LEFT JOIN gold.dim_customers AS dc
ON dc.customer_id = sd.sls_cust_id
