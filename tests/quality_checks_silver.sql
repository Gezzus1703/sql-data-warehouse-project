/*
===========================================================================
Quality Checks
===========================================================================
Script Purpose:
  This script performs various quality checks for data consistency, accuracy, 
  and standardization across the 'silver' schemas. It includes checks for:
  - Null or duplicate primary keys.
  - Unwanted spaces in string fields.
  - Data standardization and consistency.
  - Invalid date ranges and orders.
  - Data consistency between related fields.

Usage Notes:
  - Run these checks after data loading Silver Layer.
  - Investigate and resolve any discrepancies found during the checks.
===========================================================================
*/

-- ======================================================================
-- Checking 'silver.crm_cust_info'
-- ======================================================================
-- Check for NULLS or Duplicates in Primary Key
-- Expectation: No Results
SELECT
	cst_id,
	COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Check for unwanted Spaces
-- Expectation: No Results
SELECT
	cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);
SELECT
	cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);
SELECT
	cst_gndr
FROM silver.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr);

-- Data Standarization & consistency
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info;

SELECT DISTINCT cst_material_status
FROM silver.crm_cust_info

-- ======================================================================
-- Checking 'silver.crm_prd_info'
-- ======================================================================
-- Change of datatype in prd_start_dt and prd_end_dt AND add cat_id
IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
	DROP TABLE silver.crm_prd_info;
GO
  
CREATE TABLE silver.crm_prd_info (
	prd_id				INT,
	cat_id				NVARCHAR(50),
	prd_key				NVARCHAR(50),
	prd_nm				NVARCHAR(50),
	prd_cost			INT,
	prd_line			NVARCHAR(50),
	prd_start_dt		DATE,
	prd_end_dt			DATE,
	dwh_create_date		DATETIME2 DEFAULT GETDATE()
);
-- Quality Check
SELECT 
prd_id,
COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;
  
-- Check for unwanted Spaces
-- Expectation: No Results
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);
  
-- Check for NULLs or Negative Numbers
-- Expectation: No Results
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL ;
  
--Data Standarization & Consistency
SELECT DISTINCT prd_line
FROM silver.crm_prd_info;
  
-- Check for Invalid Date Orders
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;


-- ======================================================================
-- Checking 'silver.crm_prd_info'
-- ======================================================================
--Updating Datatype 
IF OBJECT_ID('silver.crm_sls_details', 'U') IS NOT NULL
	DROP TABLE silver.crm_sls_details;
GO
  
CREATE TABLE silver.crm_sls_details (
	sls_order_num	NVARCHAR(50),
	sls_prd_key		NVARCHAR(50),
	sls_cust_id		INT,
	sls_order_dt	DATE,
	sls_ship_dt		DATE,
	sls_due_dt		DATE,
	sls_sales		INT,
	sls_quantity	INT,
	sls_price		INT,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

-- Check for Invalid Date Orders
-- Expectation: No Results
SELECT *
FROM bronze.crm_sls_details
WHERE	sls_order_dt > sls_ship_dt 
OR		sls_order_dt > sls_due_dt;

-- Check Data Consistency: Between Sales, Quantitym and Price
SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price
FROM silver.crm_sls_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;


-- ======================================================================
-- Checking 'silver.erp_cust_az12'
-- ======================================================================
-- Check and transform invalid id to conect between tables
-- Expectations: No Results
SELECT
cid,
CASE 
	WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
	ELSE cid
END AS cid,
bdate,
gen
FROM bronze.erp_cust_az12
WHERE 
CASE 
	WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
	ELSE cid
END NOT IN(SELECT DISTINCT cst_key FROM silver.crm_cust_info);

-- Identify Out-of-Range Dates
SELECT DISTINCT
bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE();

-- Data Standarization & Consistency 
SELECT DISTINCT 
CASE 
	WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
	WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
	ELSE 'n/a'
END AS gen
FROM bronze.erp_cust_az12;


-- ======================================================================
-- Checking 'silver.crm_cust_info'
-- ======================================================================
-- Check and transform invalid id to conect between tables
-- Expectations: No Results
SELECT
REPLACE(cid, '-', '') cid,
cntry
FROM silver.erp_loc_a101
WHERE (REPLACE(cid, '-', '')) NOT IN 
(SELECT cst_key FROM silver.crm_cust_info)

-- Data Standarization & Consistency 
SELECT DISTINCT
cntry
FROM silver.erp_loc_a101
ORDER BY cntry
