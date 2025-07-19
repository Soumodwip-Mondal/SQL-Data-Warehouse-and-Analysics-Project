--Tables creations according to our data sources
/*
In our data source there are two source src_crm and src_erp. The tables in src_crm are cust_info,prd_info,sales_details 
and in src_erp are  CUST_AZ12, LOC_A101, PX_CAT_G1V2. 
This script will create all tables inside the bronze_layer schema
*/

CREATE TABLE bronze_layer.crm_cust_info
(
	cst_id INT,
	cst_key VARCHAR(50),
	cst_firstname VARCHAR(50),
	cst_lastname VARCHAR(50),
	cst_marital_status VARCHAR(50),
	cst_gndr VARCHAR(50),
	cst_create_date DATE
);
CREATE TABLE bronze_layer.crm_prd_info(
	prd_key INT,
	prd_nm VARCHAR(50),
	prd_cost INT,
	prd_line VARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt DATE
);
CREATE TABLE bronze_layer.crm_sales_details(
	sls_ord_num VARCHAR(50),
	sls_prd_key VARCHAR(50),
	sls_cust_id INT,
	sls_order_dt INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT
);
CREATE TABLE bronze_layer.erp_CUST_AZ12
(
	CID VARCHAR(50),
	BDATE DATE,
	GEN VARCHAR(50)
);

CREATE TABLE bronze_layer.erp_LOC_A101
(
	CID	VARCHAR(50),
	CNTRY VARCHAR(50)
);
							
CREATE TABLE bronze_layer.erp_PX_CAT_G1V2
(
	ID INT,
	CAT VARCHAR(50),
	SUBCAT VARCHAR(50),
	MAINTENANCE VARCHAR(50)
);
			
						
						
