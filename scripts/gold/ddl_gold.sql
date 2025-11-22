create or replace view gold.dim_customer
as
select
row_number () over(order by cst_id) as customer_sk,
cci.cst_id as customer_id,
cci.cst_key as customer_number,
cci.cst_firstname as first_name,
cci.cst_lastname as last_name,
ela.cntry as country,
cci.cst_marital_status as marital_status,
case 
	when cci.cst_gndr != 'N/A' then cci.cst_gndr
	else coalesce (eca.gen,'N/A')
end as gender,
eca.bdate as birth_date,
cci.cst_create_date as create_date
from silver.crm_cust_info cci
left join silver.erp_cust_az12 eca 
on cci.cst_key = eca.cid 
left join silver.erp_loc_a101 ela 
on cci.cst_key = ela.cid 



create or replace view gold.dim_product
as
select 
row_number() over(order by cpi.prd_start_dt, cpi.prd_key) as product_sk,
cpi.prd_id as product_id,
cpi.prd_key as product_number,
cpi.prd_nm as product_name,
cpi.cat_id as category_id,
epcgv.cat as category,
epcgv.subcat as sub_category,
epcgv.maintenance as maintenance,
cpi.prd_cost as product_cost,
cpi.prd_line as product_line,
cpi.prd_start_dt as product_start_date
from silver.crm_prd_info cpi
left join silver.erp_px_cat_g1v2 epcgv 
on cpi.cat_id =epcgv.id 
where prd_end_dt is null




create or replace view gold.fact_sales
as
select 
sales.sls_ord_num as order_number,
cst.customer_sk,
prd.product_sk,
sales.sls_order_dt as order_date,
sales.sls_ship_dt as shipping_date,
sales.sls_due_dt as due_date,
sales.sls_price as unit_price,
sales.sls_quantity as quantity,
sales.sls_sales as sales_amount
from silver.crm_sales_details sales
left join gold.dim_customer cst 
on cst.customer_id = sales.sls_cust_id
left join gold.dim_product prd 
on prd.product_number = sales.sls_prd_key