CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
	BEGIN TRY
		SET @batch_start_time= GETDATE();
		PRINT '============================================================'
		PRINT 'LOADING SILVER LAYER'
		PRINT '============================================================'

		PRINT '------------------------------------------------------------'
		PRINT 'LOADING CRM TABLE'
		PRINT '------------------------------------------------------------'

		SET @start_time = GETDATE();
		PRINT 'TRUNGCATING TABLE: silver.crm_cust_info'
		TRUNCATE TABLE silver.crm_cust_info;

		PRINT 'INSERTING DATA TO: silver.crm_cust_info'
		INSERT INTO silver.crm_cust_info(
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date
		)
		SELECT 
		cst_id,
		cst_key,
		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,
		CASE WHEN UPPER(TRIM(cst_marital_status))='S' then 'Single'
			 WHEN UPPER(TRIM(cst_marital_status))='M' then 'Married'
			 ELSE 'n/a'
		END cst_marital_status,
		CASE WHEN UPPER(TRIM(cst_gndr))='F' THEN 'Female'
			 WHEN UPPER(TRIM(cst_gndr))='M' THEN 'Male'
			 ELSE 'n/a'
		END cst_gndr,
		cst_create_date
		FROM
		(SELECT *,
		ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
		FROM bronze.crm_cust_info
		WHERE cst_id IS NOT NULL)t
		WHERE flag_last=1
		SET @end_time =GETDATE();
		PRINT 'LOAD DURATION: '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+ ' seconds';
		PRINT '---------------'

		SET @start_time=GETDATE();
		PRINT 'TRUNGCATING TABLE: silver.crm_prd_info'
		TRUNCATE TABLE silver.crm_prd_info;

		PRINT 'INSERTING DATA TO: silver.crm_prd_info'
		INSERT INTO silver.crm_prd_info(
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt)
		SELECT
		prd_id,
		REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
		SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_k,
		prd_nm,
		ISNULL(prd_cost,0) AS prd_cost,
		CASE UPPER(TRIM(prd_line))
			 WHEN 'S' then 'Other Sales'
			 WHEN 'M' then 'Mountain'
			 WHEN 'R' then 'Road'
			 WHEN 'T' then 'Touring'
			 ELSE 'n/a'
		END prd_line,
		CAST(prd_start_dt AS DATE) AS prd_start_dt,
		CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
		FROM bronze.crm_prd_info
		SET @end_time=GETDATE()
		PRINT 'LOAD DURATION: '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+ ' seconds';
		PRINT '---------------'

		SET @start_time=GETDATE();
		PRINT 'TRUNGCATING TABLE: silver.crm_sales_details'
		TRUNCATE TABLE silver.crm_sales_details;

		PRINT 'INSERTING DATA TO: silver.crm_sales_details'
		INSERT INTO silver.crm_sales_details(
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price)
		SELECT 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE WHEN sls_order_dt=0 OR LEN(sls_order_dt)!=8 THEN NULL
			 ELSE CAST(CAST(sls_order_dt AS varchar) AS DATE)
		END sls_order_dt,
		CASE WHEN sls_ship_dt=0 OR LEN(sls_ship_dt)!=8 THEN NULL
			 ELSE CAST(CAST(sls_ship_dt AS varchar) AS DATE) 
		END sls_ship_dt,
		CASE WHEN sls_due_dt=0 OR LEN(sls_due_dt)!=8 THEN NULL
			 ELSE CAST(CAST(sls_due_dt AS varchar) AS DATE) 
		END sls_due_dt,
		CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales!= sls_quantity*ABS(sls_price)
			 THEN sls_quantity*ABS(sls_price)
			 ELSE sls_sales
		END sls_sales,
		sls_quantity,
		CASE WHEN sls_price IS NULL OR sls_price <=0
			 THEN sls_sales/NULLIF(sls_quantity,0)
			 ELSE sls_price
		END sls_price
		FROM bronze.crm_sales_details
		SET @end_time=GETDATE()
		PRINT 'LOAD DURATION: '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+ ' seconds';
		PRINT '---------------'

		PRINT '------------------------------------------------------------'
		PRINT 'LOADING ERP TABLE'
		PRINT '------------------------------------------------------------'

		SET @start_time=GETDATE();
		PRINT 'TRUNGCATING TABLE: silver.erp_CUST_AZ12'
		TRUNCATE TABLE silver.erp_CUST_AZ12;

		PRINT 'INSERTING DATA TO: silver.erp_CUST_AZ12'
		INSERT INTO silver.erp_CUST_AZ12(CID,BDATE,GEN)
		SELECT
		CASE WHEN CID LIKE 'NAS%' then SUBSTRING(CID,4,LEN(CID))
			 ELSE CID 
		END CID,
		CONVERT(DATE,BDATE,103) AS BDATE,
		CASE WHEN TRIM(GEN) IN ('F','Female') THEN 'Female'
			 WHEN TRIM(GEN) IN ('M','Male') THEN 'Male'
			 ELSE 'n/a'
		END GEN
		FROM bronze.erp_CUST_AZ12
		SET @end_time=GETDATE()
		PRINT 'LOAD DURATION: '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+ ' seconds';
		PRINT '---------------'

		SET @start_time=GETDATE();
		PRINT 'TRUNGCATING TABLE: silver.erp_LOC_A101'
		TRUNCATE TABLE silver.erp_LOC_A101;

		INSERT INTO silver.erp_LOC_A101(
		CID,
		CNTRY
		)
		SELECT 
		TRIM(REPLACE(CID,'-','')) AS CID, 
		CASE WHEN TRIM(CNTRY) = 'DE' THEN 'Germany'
			 WHEN TRIM(CNTRY) IN ('US','USA') THEN 'United States'
			 WHEN TRIM(CNTRY) IS NULL or TRIM(CNTRY)='' THEN 'n/a'
			 ELSE TRIM(CNTRY)
		END CNTRY
		FROM bronze.erp_LOC_A101
		SET @end_time=GETDATE()
		PRINT 'LOAD DURATION: '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+ ' seconds';
		PRINT '---------------'

		SET @start_time=GETDATE();
		PRINT 'TRUNGCATING TABLE: silver.erp_PX_CAT_G1V2'
		TRUNCATE TABLE silver.erp_PX_CAT_G1V2;

		PRINT 'ISERTING DATA TO: silver.erp_PX_CAT_G1V2'
		INSERT INTO silver.erp_PX_CAT_G1V2(
		ID,
		CAT,
		SUBCAT,
		MAINTENANCE)
		SELECT
		ID,
		CAT,
		SUBCAT,
		MAINTENANCE
		FROM bronze.erp_PX_CAT_G1V2
		SET @end_time=GETDATE()
		PRINT 'LOAD DURATION: '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+ ' seconds';
		PRINT '---------------'
		
		SET @batch_end_time=GETDATE();
		PRINT '===================================================='
		PRINT 'LOADING SILVER LAYER IS COMPLETED'
		PRINT 'TOTAL LOAD DURATION: ' +CAST(DATEDIFF(second,@batch_start_time,@batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '===================================================='
	END TRY
	BEGIN CATCH
		PRINT '===================================================='
		PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER'
		PRINT 'Error Message ' + ERROR_MESSAGE();
		PRINT 'Error Number ' + CAST(ERROR_NUMBER() AS NVARCHAR); 
		PRINT 'Error State ' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '===================================================='
	END CATCH
END
GO
EXEC silver.load_silver
