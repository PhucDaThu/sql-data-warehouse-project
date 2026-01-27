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
