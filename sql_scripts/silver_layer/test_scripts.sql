--test scripts for data transformation . In this script I have written all test scripts for each ste of this project
select *
from bronze_layer.crm_cust_info;
--Checking if there exists any duplicates
select count(*)-count(distinct(cst_id)) as diff
from bronze_layer.crm_cust_info;

-- All the cst_id who have duplicates

select cst_id,count(cst_id)
from bronze_layer.crm_cust_info
group by cst_id
having count(cst_id)>1;
-- Checking for other columns 
-- cst_firstname
select cst_firstname
from bronze_layer.crm_cust_info
where cst_firstname is null or cst_firstname !=trim(cst_firstname);
-- checking for last name
select cst_lastname
from bronze_layer.crm_cust_info
where cst_lastname is null or cst_lastname !=trim(cst_lastname);
-- Checking for meratial status
select distinct cst_marital_status
from bronze_layer.crm_cust_info;
--Checking for customer's gender
select distinct cst_gndr
from bronze_layer.crm_cust_info;

select * 
from (select *, row_number() over(partition by cst_id order by cst_create_date desc) as rnk
from bronze_layer.crm_cust_info)
where rnk=1;

select *
from bronze_layer.crm_sales_details;
select *
from bronze_layer.erp_px_cat_g1v2;
select *
from silver_layer.crm_prd_info;
select  replace(SUBSTRING( prd_key FROM 1 FOR 5),'-','_') as cat_id,SUBSTRING( prd_key FROM 7 FOR length(prd_key)) as prd_key
from bronze_layer.crm_prd_info;
-- checking for prd_nm column
select prd_nm
from bronze_layer.crm_prd_info
where prd_nm <> trim(prd_nm)
-- checking for product cost
select prd_cost 
from bronze_layer.crm_prd_info
where prd_cost<0 or prd_cost is null;

--replaceing nulls with 0
select
case
	 when prd_cost is null then 0
	 else prd_cost
	 end as prd
from bronze_layer.crm_prd_info; 

-- checking for prd_line
select distinct prd_line
from bronze_layer.crm_prd_info;
-- replaceing the product line values(Short forms) with known value

select  product_line_name
from (
select 
	CASE UPPER(TRIM(prd_line))
    WHEN 'M' THEN 'Machinery'
    WHEN 'R' THEN 'Retail'
    WHEN 'S' THEN 'Sports'
    WHEN 'T' THEN 'Technology'
    ELSE 'Unknown'
END AS product_line_name
from bronze_layer.crm_prd_info
)
select *
from bronze_layer.crm_prd_info;
select prd_start_dt
from bronze_layer.crm_prd_info;
select prd_end_dt
from bronze_layer.crm_prd_info;
select prd_start_dt,prd_end_dt
from bronze_layer.crm_prd_info
where prd_start_dt > prd_end_dt
order by prd_start_dt;

select prd_start_dt,
case 
	when prd_start_dt > prd_end_dt then lead(prd_start_dt) over(partition by prd_nm order by prd_start_dt)-1 
	else prd_end_dt 
	end as prd_end_date
from bronze_layer.crm_prd_info;

