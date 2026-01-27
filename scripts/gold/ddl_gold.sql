/*
==============================================================================================
Tạo view cho gold layer
==============================================================================================
Mục đích script:
    chạy script để tạo ra views cho gold layer trong data warehouse. Gold layer đại diện cho 
    các bảng dimension và fact cuối cùng (theo mô hình Star Schema).Mỗi View thực hiện các phép 
    biến đổi và kết hợp dữ liệu từ tầng Silver để tạo ra một tập dữ liệu sạch, giàu thông tin 
    và sẵn sàng cho mục đích kinh doanh.
Sử dụng
    Các View có thể được sử dụng trực tiếp cho phân tích và báo cáo
===============================================================================================
*/

IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO
CREATE VIEW gold.dim_customers AS
SELECT
ROW_NUMBER() OVER (ORDER BY cs.cst_id) AS customer_key,
cs.cst_id AS customer_id,
cs.cst_key AS customer_number,
cs.cst_firstname AS first_name,
cs.cst_lastname AS last_name,
lc.CNTRY AS country,
cs.cst_marital_status AS martial_status,
CASE WHEN cs.cst_gndr!='n/a' THEN cs.cst_gndr
	 ELSE COALESCE(ct.gen,'n/a')
END AS gender,
ct.BDATE AS birthday,
cs.cst_create_date AS create_date
FROM silver.crm_cust_info cs
LEFT JOIN silver.erp_CUST_AZ12 ct
ON cs.cst_key=ct.CID
LEFT JOIN silver.erp_LOC_A101 lc
ON cs.cst_key=lc.CID
GO

IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO
CREATE VIEW gold.dim_products AS 
SELECT 
ROW_NUMBER() OVER (ORDER BY pt.prd_start_dt,pt.prd_key) AS product_key,
pt.prd_id AS product_id,
pt.prd_key AS product_number,
pt.prd_nm AS product_name,
pt.cat_id AS category_id,
px.CAT AS category,
px.SUBCAT AS subcategory,
px.MAINTENANCE AS maintenance,
pt.prd_cost AS cost,
pt.prd_line AS product_line,
pt.prd_start_dt AS start_date
FROM silver.crm_prd_info pt
LEFT JOIN silver.erp_PX_CAT_G1V2 px
ON pt.cat_id=px.ID
WHERE prd_end_dt IS NULL -- FILTER OUT ALL HISTORICAL DATA
GO

IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO
CREATE VIEW gold.fact_sales AS
SELECT 
sls_ord_num AS order_number,
product_key,
customer_key,
sls_order_dt AS order_date,
sls_ship_dt AS shipping_date,
sls_due_dt AS due_date,
sls_sales AS sales_amount,
sls_quantity AS quantity,
sls_price AS price
FROM silver.crm_sales_details
LEFT JOIN gold.dim_products
ON  product_number = sls_prd_key
LEFT JOIN gold.dim_customers
ON customer_id= sls_cust_id
