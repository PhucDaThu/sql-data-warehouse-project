/*
====================================
tạo database tên DataWarehouse 
====================================
Mục đích của scripts: 
  Tạo ra database mang tên "DataWarehouse", đầu tiên ta kiểm tra xem đã tồn tại database đó chưa. 
  Nếu rồi thì xóa và tạo lại database đó. Sau khi tạo xong hãy kết nối tới Database DataWarehouse và chạy tiếp lệnh
  trong init_schemas.sql để tạo ra 3 schemas lần lượt mang tên bronze, silver, gold.
Lưu ý:
  Sử dụng scripts này sẽ xóa đi tất cả dữ liệu của database: "DataWarehouse" đã tồn tại.
  Hãy đảm bảo bạn đã backup mọi dữ liệu trước khi chạy scripts này.
  Hãy chạy từng câu lệnh một vì trong PgAdmin không chạy được lệnh DROP và CREATE trên cùng một khối
*/
DROP Database IF EXISTS DataWarehouse;
CREATE Database DataWarehouse;
