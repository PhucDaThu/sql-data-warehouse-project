/*
====================================
tạo database tên DataWarehouse 
====================================
Mục đích của scripts: 
  Tạo ra database mang tên "DataWarehouse", đầu tiên ta kiểm tra xem đã tồn tại database đó chưa. 
  Nếu rồi thì xóa và tạo lại database đó. Sau đó tạo ra 3 schemas lần lượt mang tên bronze, silver, gold.
Lưu ý:
  Sử dụng scripts này sẽ xóa đi tất cả dữ liệu của database: "DataWarehouse" đã tồn tại.
  Hãy đảm bảo bạn đã backup mọi dữ liệu trước khi chạy scripts này.
*/
USE master
GO

IF EXISTS (SELECT 1 FROM sys.databases WHERE name= 'DataWarehouse')
BEGIN
  ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
  DROP DATABASE DataWarehouse;
END;
GO

CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO





