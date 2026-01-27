/*
=================================================================================
QUALITY CHECKS
=================================================================================
Mục đích của script:
	Kiểm tra lại dữ liệu của toàn bộ bảng silver bao gồm:
	- Kiểm tra trong khóa chính có giá trị NULL hay bị trùng lặp không
	- Kiểm tra cách khoảng trắng không cần thiết
	- Kiểm tra tính chuẩn hóa và tính nhất quán
	- Kiểm tra tính hợp lệ của ngày tháng năm
Sử dụng:
	Chạy script sau khi đã tải dữ liệu vào silver
	Phát hiện để sửa chữa bất kì sai sót nào trong quá trình kiểm tra
=================================================================================
*/

--===============================================================================
-- CHECKING silver.crm_cust_info
--===============================================================================
SELECT * FROM silver.crm_cust_info
ORDER BY cst_id

-- CHECK FOR DUPLICATE OR NULL IN PRIMARY KEY
-- EXPECTATION: NO RESULTS
SELECT cst_id,COUNT(*) 
FROM silver.crm_cust_info
GROUP BY cst_id 
HAVING cst_id IS NULL or COUNT(*) >1

-- CHECK FOR UNWANTED SPACE
-- EXPECTATION: NO RESULTS
SELECT cst_key,cst_firstname,cst_lastname
FROM silver.crm_cust_info
WHERE TRIM(cst_key) != cst_key OR TRIM(cst_firstname) != cst_firstname OR TRIM(cst_lastname) != cst_lastname

-- DATA STANDARDIZATION AND CONSISTENCY
-- EXPECTATION: NO NULL VALUES
SELECT DISTINCT cst_marital_status,cst_gndr
FROM silver.crm_cust_info

--===============================================================================
-- CHECKING silver.crm_prd_info
--===============================================================================
SELECT * FROM silver.crm_prd_info
ORDER BY prd_id

-- CHECK FOR DUPLICATE OR NULL IN PRIMARY KEY
-- EXPECTATION: NO RESULTS
SELECT prd_id,COUNT(*) 
FROM silver.crm_prd_info
GROUP BY prd_id 
HAVING prd_id IS NULL or COUNT(*) >1

-- CHECK FOR UNWANTED SPACE
-- EXPECTATION: NO RESULTS
SELECT prd_key,prd_nm
FROM silver.crm_prd_info
WHERE TRIM(prd_nm) != prd_nm OR TRIM(prd_key) != prd_key 

-- CHECK FOR NULL OR NEGATIVE VALUES IN COST
-- EXPECTATION: NO RESULTS
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost IS NULL or prd_cost < 0

-- DATA STANDARDIZATION AND CONSISTENCY
-- EXPECTATION: NO NULL VALUES
SELECT DISTINCT prd_line
FROM silver.crm_prd_info

--===============================================================================
-- CHECKING silver.crm_sales_details
--===============================================================================
SELECT * FROM silver.crm_sales_details

-- CHECK FOR INVALID DATES
-- EXPECTATION: NO RESULTS
SELECT sls_order_dt
FROM silver.crm_sales_details
WHERE sls_order_dt < '1950-01-01'
OR sls_order_dt > '2050-01-01'

-- CHECK FOR INVALID DATES ORDER (order dates > ship/due dates)
-- EXPECTATION: NO RESULTS
SELECT sls_order_dt
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt 
OR sls_ship_dt > sls_due_dt
OR sls_order_dt > sls_due_dt

-- CHECK FOR BUSINESS RULE:
-- EXPECTATION: NO RESULTS
SELECT sls_sales,sls_quantity,sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <=0 OR sls_quantity <=0 OR sls_price <=0

--===============================================================================
-- CHECKING silver.erp_CUST_AZ12
--===============================================================================
SELECT * FROM silver.erp_CUST_AZ12
ORDER BY CID

-- CHECK FOR UNWANTED SPACE
-- EXPECTATION: NO RESULTS
SELECT CID
FROM silver.erp_CUST_AZ12
WHERE CID != TRIM(CID)

-- CHECK FOR INVALID DATES
-- EXPECTATION: NO RESULTS
SELECT BDATE
FROM silver.erp_CUST_AZ12
WHERE BDATE > GETDATE() 

-- CHECK FOR STANDARDIZATION AND CONSISTENCY
-- EXPECTATION: NO NULLVALUES
SELECT DISTINCT GEN
FROM silver.erp_CUST_AZ12

--===============================================================================
-- CHECKING silver.erp_LOC_A101
--===============================================================================
SELECT * FROM silver.erp_LOC_A101
ORDER BY CID 

-- CHECK FOR UNWANTED SPACE
-- EXPECTATION: NO RESULTS
SELECT CID
FROM silver.erp_LOC_A101
WHERE CID != TRIM(CID)

-- CHECK FOR STANDARDIZATION AND CONSISTENCY
-- EXPECTATION: NO NULLVALUES
SELECT DISTINCT CNTRY
FROM silver.erp_LOC_A101

--===============================================================================
-- CHECKING silver.erp_PX_CAT_G1V2
--===============================================================================
SELECT * FROM silver.erp_PX_CAT_G1V2

-- CHECK FOR UNWANTED SPACE
-- EXPECTATION: NO RESULTS
SELECT ID,CAT,SUBCAT
FROM silver.erp_PX_CAT_G1V2
WHERE ID != TRIM(ID) OR CAT != TRIM(CAT) OR SUBCAT != TRIM(SUBCAT)

-- CHECK FOR STANDARDIZATION AND CONSISTENCY
-- EXPECTATION: NO NULL VALUES
SELECT DISTINCT MAINTENANCE
FROM silver.erp_PX_CAT_G1V2
