--- silver.crm_cust_info
-- Check for duplicated or NULL id
SELECT 
	cst_id,
	COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Check for unwated spaces
SELECT
	cst_key
FROM silver.crm_cust_info
WHERE TRIM(cst_key) != cst_key;

-- Check for data consistency
SELECT DISTINCT
	cst_marital_status
FROM silver.crm_cust_info;

--- silver.crm_prd_info
-- Check for duplicated or NULL id
SELECT
	prd_id,
	COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Check for unwated spaces
SELECT
	prd_nm
FROM silver.crm_prd_info
WHERE TRIM(prd_nm) != prd_nm

-- Check for invalid cost (negative or NULL)
SELECT
	prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Check for data incosistency
SELECT DISTINCT
	prd_line
FROM silver.crm_prd_info

-- Check for invalid date order
SELECT
	*
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt

--- silver.crm_sales_details
-- Check for invalid date order
SELECT
	NULLIF(sls_due_dt, 0) AS sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0
   OR LEN(sls_due_dt) != 8
   OR sls_due_dt > 20260101
   OR sls_due_dt < 19000101

SELECT
	*
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt
   OR sls_order_dt > sls_due_dt;

-- Check for data consistency following the rule: Sales = Quantity * Price
SELECT
	sls_sales,
	sls_quantity,
	sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL
   OR sls_quantity IS NULL
   OR sls_price IS NULL
   OR sls_sales <= 0
   OR sls_quantity <= 0
   OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price

--- silver.erp_cust_az12
-- Check for abnormal birthdates
SELECT DISTINCT
	bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01'
   OR bdate > GETDATE();

-- Check for data consistency
SELECT DISTINCT
	gen
FROM silver.erp_cust_az12;

--- silver.erp_loc_a101
-- Check for data consistency
SELECT DISTINCT
	cntry
FROM silver.erp_loc_a101
ORDER BY cntry;

--- silver.erp_px_cat_g1v2
-- Check for unwanted space
SELECT
	*
FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat)
	OR subcat != TRIM(subcat)
	OR maintenance != TRIM(maintenance);

-- Check for data consistency
SELECT DISTINCT
	maintenance
FROM silver.erp_px_cat_g1v2;
