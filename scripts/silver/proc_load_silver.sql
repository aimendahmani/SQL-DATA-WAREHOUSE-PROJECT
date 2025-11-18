create or replace procedure silver.load_silver()
language plpgsql
as $$
declare

	v_start_time timestamp;
	v_end_time timestamp;
	batch_start_time timestamp;
	batch_end_time timestamp;
	error_code text;
	error_message text;

begin
	
	batch_start_time := clock_timestamp();
	

insert into log.stepslog(procedure_name,step,start_time,end_time,elapsed_time,error_code,error_message)
values('bronze.load_silver', 'start',batch_start_time,null,null,null,null);





-- silver layer

-- 1. crm

-- 1.1 customer informations

v_start_time := clock_timestamp();

insert into log.stepslog(procedure_name,step,start_time,end_time,elapsed_time,error_code,error_message)
values('silver.load_silver', 'insert into silver.crm_cust_info',v_start_time,null,null,null,null);

-- *** truncate table ***
truncate table silver.crm_cust_info; 
-- *** insert into table ***
insert into silver.crm_cust_info 
select 
cst_id,
cst_key,
trim(cst_firstname),
trim(cst_lastname),
case
	when upper(cst_marital_status)='M' then 'Married'
	when upper(cst_marital_status)='S' then 'Single'
	else 'N/A'
end cst_marital_status,
case
	when upper(cst_gndr)='M' then 'Male'
	when upper(cst_gndr)='F' then 'Female'
	else 'N/A'
end cst_gndr,
cst_create_date
from 
(
select *, row_number() over(partition by cst_id order by cst_create_date desc) as flag_last from bronze.crm_cust_info

) where flag_last=1;

v_end_time := clock_timestamp();

update log.stepslog 
set end_time = v_end_time,
	elapsed_time = extract(epoch from (v_end_time-v_start_time))
where procedure_name ='silver.load_silver'
and step = 'insert into silver.crm_cust_info'
and end_time is null;

-- 1.2 product information

v_start_time := clock_timestamp();

insert into log.stepslog(procedure_name,step,start_time,end_time,elapsed_time,error_code,error_message)
values('silver.load_silver', 'insert into silver.crm_prd_info',v_start_time,null,null,null,null);

-- *** truncate table ***

truncate table bronze.crm_prd_info;

-- *** insert into table *** 

insert into silver.crm_prd_info 
select
prd_id,
substring(prd_key,1,5) as cat_id,
substring(prd_key,7,length(prd_key)) as prd_key,
trim(prd_nm),
coalesce(prd_cost,'0,00 DA'),
case upper(trim(prd_line))
	when 'M' then 'Mountain'
	when 'R' then 'Road'
	when 'S' then 'Other sales'
	when 'T' then 'Touring'
	else 'N/A'
end as prd_line,
prd_start_dt::date as prd_start_dt,
lead(prd_start_dt) over(partition by prd_key order by prd_start_dt asc)::date -1 as prd_end_dt
from bronze.crm_prd_info;

v_end_time := clock_timestamp();

update log.stepslog 
set end_time = v_end_time,
	elapsed_time = extract(epoch from (v_end_time-v_start_time))
where procedure_name ='silver.load_silver'
and step = 'insert into silver.crm_prd_info'
and end_time is null;

-- 1.3 sales information

v_start_time := clock_timestamp();

insert into log.stepslog(procedure_name,step,start_time,end_time,elapsed_time,error_code,error_message)
values('silver.load_silver', 'insert into silver.crm_sales_details',v_start_time,null,null,null,null);

-- *** truncate table ***
truncate table silver.crm_sales_details;

-- *** insert into table ***

insert into silver.crm_sales_details 
select 
sls_ord_num,
sls_prd_key,
sls_cust_id,
case 
	when sls_order_dt <0 or length(sls_order_dt::varchar)!=8
	then null 
	else (sls_order_dt::varchar)::date 
end sls_order_dt,
case
	when sls_ship_dt < 0 or length(sls_ship_dt::varchar)!=8
	then null 
	else (sls_ship_dt::varchar)::date
end sls_ship_dt,
case 
	when sls_due_dt < 0 or length(sls_due_dt::varchar)!=8
	then null 
	else (sls_due_dt::varchar)::date
end sls_due_dt,
case 
	when sls_sales is null or sls_sales <= 0 or sls_quantity * abs(sls_price::decimal)!=sls_sales
	then sls_quantity * abs(sls_price::decimal)
	else sls_sales 
end sls_sales,
sls_quantity,
case 
	when sls_price is null or sls_price::decimal <=0
	then sls_sales / nullif(sls_quantity,0) 
	else sls_price::decimal
end sls_price

from bronze.crm_sales_details;

v_end_time := clock_timestamp();

update log.stepslog 
set end_time = v_end_time,
	elapsed_time = extract(epoch from (v_end_time-v_start_time))
where procedure_name ='silver.load_silver'
and step = 'insert into silver.crm_sales_details'
and end_time is null;


-- 2. ERP

-- 2.1 customer information

v_start_time := clock_timestamp();

insert into log.stepslog(procedure_name,step,start_time,end_time,elapsed_time,error_code,error_message)
values('silver.load_silver', 'insert into silver.erp_cust_az12',v_start_time,null,null,null,null);

-- *** truncate table ***

truncate silver.erp_cust_az12;

-- *** insert into table ***

insert into silver.erp_cust_az12 
select 
case 
	when cid like 'NAS%' then substring(cid,4,length(cid))
	else cid
end cid,
case 
	when bdate > current_date then null
	else bdate
end bdate,
case
	when upper(gen) in ('M','MALE') then 'Male'
	when upper(gen) in ('F','FEMALE') then 'Female'
	else 'N/A'
end gen
from bronze.erp_cust_az12;

v_end_time := clock_timestamp();

update log.stepslog 
set end_time = v_end_time,
	elapsed_time = extract(epoch from (v_end_time-v_start_time))
where procedure_name ='silver.load_silver'
and step = 'insert into silver.erp_cust_az12'
and end_time is null;

-- 2.2 customer location

v_start_time := clock_timestamp();

insert into log.stepslog(procedure_name,step,start_time,end_time,elapsed_time,error_code,error_message)
values('silver.load_silver', 'insert into silver.erp_loc_a101',v_start_time,null,null,null,null);

-- *** truncate table ***

truncate silver.erp_loc_a101;

-- *** insert into table ***

insert into silver.erp_loc_a101 
select 
replace(cid,'-','') as cid,
case 
	when cntry='DE' then 'Germany'
	when cntry in ('US','USA') then 'United States'
	when cntry is null or cntry ='' then 'N/A'
	else cntry
end cntry
from bronze.erp_loc_a101;

v_end_time := clock_timestamp();

update log.stepslog 
set end_time = v_end_time,
	elapsed_time = extract(epoch from (v_end_time-v_start_time))
where procedure_name ='silver.load_silver'
and step = 'insert into silver.erp_loc_a101'
and end_time is null;

-- 2.3 category information

v_start_time := clock_timestamp();

insert into log.stepslog(procedure_name,step,start_time,end_time,elapsed_time,error_code,error_message)
values('silver.load_silver', 'insert into silver.erp_px_cat_g1v2',v_start_time,null,null,null,null);

-- *** truncate table ***

truncate silver.erp_px_cat_g1v2;

-- *** insert into table ***

insert into silver.erp_px_cat_g1v2 
select 
replace(id,'_','-') id,
trim(cat) cat,
trim(subcat) subcat,
maintenance 
from bronze.erp_px_cat_g1v2;

v_end_time := clock_timestamp();

update log.stepslog 
set end_time = v_end_time,
	elapsed_time = extract(epoch from (v_end_time-v_start_time))
where procedure_name ='silver.load_silver'
and step = 'insert into silver.erp_px_cat_g1v2'
and end_time is null;

end; 
$$

