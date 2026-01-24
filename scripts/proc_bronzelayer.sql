/*
=================================================================================================
Procedure: Tải dữ liệu từ nguồn vào Layer Bronze
=================================================================================================
Mục đích script:
	Tạo ra một procedure lưu trữ để tải dữ liệu từ các file CSV bên ngoài vào schema 'bronze'.
	Đầu tiên kiểm tra xem dữ liệu có tồn tại trong bảng không, nếu có thì xóa đi các dữ liệu.
	Sau đó, sử dụng lệnh 'BULK INSERT' để nạp dữ liệu từ các file CSV vào bảng trong 'bronze'
	Sau khi tạo xong thì sử dụng hàm EXEC để thực thi procedure, nếu khởi tạo không thành công
  màn hình sẽ hiện thông báo lỗi cũng như thông tin lỗi.
Tham số:
	Không nhận tham số đầu vào và không trả về giá trị nào
Lưu ý:
	Sử dụng script sẽ xóa đi tất cả giá trị hiện có trong bảng để có thể nạp dữ liệu từ file CSV 
=================================================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
	BEGIN TRY
		SET @batch_start_time= GETDATE();
		PRINT '============================================================'
		PRINT 'LOADING BRONZE LAYER'
		PRINT '============================================================'

		PRINT '------------------------------------------------------------'
		PRINT 'LOADING CRM TABLE'
		PRINT '------------------------------------------------------------'

		SET @start_time = GETDATE();
		PRINT 'TRUNGCATING TABLE: bronze.crm_cut_info'
		TRUNCATE TABLE bronze.crm_cut_info;

		PRINT 'INSERTING DATA TO: bronze.crm_cut_info'
		BULK INSERT bronze.crm_cut_info
		FROM 'D:\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		)
		SET @end_time =GETDATE();
		PRINT 'LOAD DURATION: '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+ ' seconds';
		PRINT '---------------'

		SET @start_time=GETDATE();
		PRINT 'TRUNGCATING TABLE: bronze.crm_prd_info'
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT 'INSERTING DATA TO: bronze.crm_prd_info'
		BULK INSERT bronze.crm_prd_info
		FROM 'D:\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR =',',
			TABLOCK
		)
		SET @end_time=GETDATE()
		PRINT 'LOAD DURATION: '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+ ' seconds';
		PRINT '---------------'

		SET @start_time=GETDATE();
		PRINT 'TRUNGCATING TABLE: bronze.crm_sales_details'
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT 'INSERTING DATA TO: bronze.crm_sales_details'
		BULK INSERT bronze.crm_sales_details
		FROM 'D:\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		)
		SET @end_time=GETDATE()
		PRINT 'LOAD DURATION: '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+ ' seconds';
		PRINT '---------------'

		PRINT '------------------------------------------------------------'
		PRINT 'LOADING ERP TABLE'
		PRINT '------------------------------------------------------------'

		SET @start_time=GETDATE();
		PRINT 'TRUNGCATING TABLE: bronze.erp_CUST_AZ12'
		TRUNCATE TABLE bronze.erp_CUST_AZ12;

		PRINT 'INSERTING DATA TO: bronze.erp_CUST_AZ12'
		BULK INSERT bronze.erp_CUST_AZ12
		FROM 'D:\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		)
		SET @end_time=GETDATE()
		PRINT 'LOAD DURATION: '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+ ' seconds';
		PRINT '---------------'

		SET @start_time=GETDATE();
		PRINT 'TRUNGCATING TABLE: bronze.erp_LOC_A101'
		TRUNCATE TABLE bronze.erp_LOC_A101;

		PRINT 'INSERTING DATA TO: bronze.erp_LOC_A101'
		BULK INSERT bronze.erp_LOC_A101
		FROM 'D:\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		)
		SET @end_time=GETDATE()
		PRINT 'LOAD DURATION: '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+ ' seconds';
		PRINT '---------------'

		SET @start_time=GETDATE();
		PRINT 'TRUNGCATING TABLE: bronze.erp_PX_CAT_G1V2'
		TRUNCATE TABLE bronze.erp_PX_CAT_G1V2;

		PRINT 'ISERTING DATA TO: bronze.erp_PX_CAT_G1V2'
		BULK INSERT bronze.erp_PX_CAT_G1V2
		FROM 'D:\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		)
		SET @end_time=GETDATE()
		PRINT 'LOAD DURATION: '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+ ' seconds';
		PRINT '---------------'
		
		SET @batch_end_time=GETDATE();
		PRINT '===================================================='
		PRINT 'LOADING BRONZE LAYER IS COMPLETED'
		PRINT 'TOTAL LOAD DURATION: ' +CAST(DATEDIFF(second,@batch_start_time,@batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '===================================================='
	END TRY
	BEGIN CATCH
		PRINT '===================================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message ' + ERROR_MESSAGE();
		PRINT 'Error Number ' + CAST(ERROR_NUMBER() AS NVARCHAR); 
		PRINT 'Error State ' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '===================================================='
	END CATCH
END
GO
EXEC bronze.load_bronze
