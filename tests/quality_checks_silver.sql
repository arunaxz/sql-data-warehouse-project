
--Check for NULLS or DUPLICATE in primary key
--EXPECTATIO: no result
SELECT 
	prd_id, 
	COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL

--CHECK UNWANTED SAPACES
SELECT prd_nm FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)


--check for null values in cost
SELECT prd_cost FROM bronze.crm_prd_info
WHERE prd_cost IS NULL OR prd_cost < 0

SELECT DISTINCT prd_line FROM bronze.crm_prd_info

--Checking for invalid order dates
--End data should not be earlier than the start date
SELECT * FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt


SELECT prd_id
      ,prd_key
      ,prd_nm
      ,prd_start_dt
      ,prd_end_dt
	  ,LEAD(prd_start_dt) OVER(PARTITION BY prd_key  ORDER BY prd_start_dt) -1  AS new_end_date
  FROM bronze.crm_prd_info
  WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509') 

--SILVER LAYER
SELECT prd_id FROM silver.crm_prd_info
WHERE prd_id != prd_id


SELECT 
	prd_id, 
	COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL

SELECT prd_cost FROM silver.crm_prd_info
WHERE prd_cost IS NULL OR prd_cost < 0

SELECT * FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt


--BRONZE LAYER - crm_sales_details
SELECT sls_ord_num
      ,sls_prd_key
      ,sls_cust_id
      ,sls_order_dt
      ,sls_ship_dt
      ,sls_due_dt
      ,sls_sales
      ,sls_quantity
      ,sls_price
  FROM bronze.crm_sales_details
  WHERE sls_ord_num != TRIM(sls_ord_num)

SELECT sls_ord_num
      ,sls_prd_key
      ,sls_cust_id
      ,sls_order_dt
      ,sls_ship_dt
      ,sls_due_dt
      ,sls_sales
      ,sls_quantity
      ,sls_price
  FROM bronze.crm_sales_details
  WHERE sls_prd_key NOT IN (SELECT prd_key from silver.crm_prd_info)

--Checking for the invalid dates

--check if the dates are 0 and changing them to null
SELECT NULLIF(sls_due_dt, 0) 
FROM bronze.crm_sales_details
WHERE sls_due_dt = 0
OR LEN(sls_due_dt) != 8


--CHECKIN INVALID DATE ORDERS
SELECT * FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

--CHECKING INVALID PRICES 

SELECT DISTINCT 
      sls_sales
      ,sls_quantity
      ,sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price 
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales ,sls_quantity ,sls_price

SELECT * FROM silver.crm_prd_info
SELECT * FROM bronze.erp_px_cat_g1v2
SELECT * FROM silver.crm_sales_details

-- erp_cust_az12
SELECT 
		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
			ELSE cid
		END AS cid
      ,bdate
      ,gen
  FROM bronze.erp_cust_az12
  WHERE CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
			ELSE cid
		END NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info)

--Checking birth days that are out of boundaries
SELECT DISTINCT bdate
FROM silver.erp_cust_az12
WHERE bdate > GETDATE()

SELECT DISTINCT 

	 CASE	WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'FEMALE'
			WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'MALE'
			ELSE 'n/a'
	END AS gen
FROM silver.erp_cust_az12

--bronze.erp_loc_a101

SELECT REPLACE(cid, '-', '') cid
      ,cntry
  FROM bronze.erp_loc_a101
  WHERE REPLACE(cid, '-', '') NOT IN( SELECT cst_key FROM silver.crm_cust_info)

--country quality check
SELECT DISTINCT cntry
  FROM (SELECT CASE 
			WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
			WHEN TRIM(cntry) = '' OR Cntry IS NULL THEN 'n/a'
			ELSE TRIM(cntry)
		END AS cntry
FROM bronze.erp_loc_a101)t

SELECT DISTINCT * FROM silver.erp_loc_a101

--erp_px_cat_g1v2
SELECT id
      ,cat
      ,subcat
      ,maintenance
  FROM bronze.erp_px_cat_g1v2

  --checking for unwantted spaces
  SELECT *
  FROM bronze.erp_px_cat_g1v2
  WHERE TRIM(cat) != cat OR TRIM(subcat) != subcat OR TRIM(maintenance) != maintenance

--checking consistency and satandard

  SELECT DISTINCT *
  FROM silver.erp_px_cat_g1v2
