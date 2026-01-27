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
