

-- Load the cleaned,standard data into silver_layer.crm_cust_info table from bronze_layer.crm_cust_info 
TRUNCATE TABLE silver_layer.crm_cust_info;
INSERT INTO silver_layer.crm_cust_info (
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
    CASE 
        WHEN cst_firstname <> TRIM(cst_firstname) THEN TRIM(cst_firstname)
        WHEN cst_firstname IS NULL THEN 'n/a'
        ELSE cst_firstname
    END AS cst_firstname,
    CASE 
        WHEN cst_lastname <> TRIM(cst_lastname) THEN TRIM(cst_lastname)
        WHEN cst_lastname IS NULL THEN 'n/a'
        ELSE cst_lastname
    END AS cst_lastname,
    CASE 
        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
		WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        ELSE COALESCE(cst_marital_status, 'n/a')
    END AS cst_marital_status,
    CASE 
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
		WHEN UPPER(TRIM(cst_gndr))='M' THEN 'Male'
        ELSE 'n/a'
    END AS cst_gndr,
    cst_create_date
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS rnk
    FROM bronze_layer.crm_cust_info
) t
WHERE rnk = 1 AND cst_id IS NOT NULL;
select *
from silver_layer.crm_cust_info;
select(
(select count(*)
from bronze_layer.crm_cust_info)
-
(select count(*)
from silver_layer.crm_cust_info)
) as diff
select distinct cst_gndr
from silver_layer.crm_cust_info;


-- We have to update the silver_layer.crm_prd_info
drop table if exists silver_layer.crm_prd_info ;
create table silver_layer.crm_prd_info (
	prd_id INT,
	cat_id VARCHAR(50),
	prd_key VARCHAR(50),
	prd_nm VARCHAR(100),
	prd_cost INT,
	prd_line VARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt DATE,
	prd_update_dt DATE DEFAULT NOW()::date
)
TRUNCATE TABLE silver_layer.crm_prd_info;
INSERT INTO silver_layer.crm_prd_info(prd_id,cat_id,prd_key,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt)
(
	select prd_id,
	replace(SUBSTRING( prd_key FROM 1 FOR 5),'-','_') as cat_id,
	SUBSTRING( prd_key FROM 7 FOR length(prd_key)) as prd_key,
	trim(prd_nm),
	case
	 when prd_cost is null then 0
	 else prd_cost
	 end as prd_cost
	,
	CASE UPPER(TRIM(prd_line))
	    WHEN 'M' THEN 'Machinery'
	    WHEN 'R' THEN 'Retail'
	    WHEN 'S' THEN 'Sports'
	    WHEN 'T' THEN 'Technology'
	    ELSE 'Unknown'
		END AS prd_line
	,
	prd_start_dt,
	case 
	when prd_start_dt > prd_end_dt then lead(prd_start_dt) over(partition by prd_nm order by prd_start_dt)-1 
	else prd_end_dt 
	end as prd_end_dt
	from bronze_layer.crm_prd_info
);

