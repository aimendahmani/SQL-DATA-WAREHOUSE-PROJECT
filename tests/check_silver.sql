-- silver layer --

-- 1 crm 

-- 1.1 customer informations

-- *** check for duplicated or null cst_id ***

select cst_id, count(*) from 
silver.crm_cust_info
group by 1
having count(*) > 1 or cst_id is null

-- 1.2 product information

-- *** check for duplicated or null prd_id ***

SELECT prd_id, count(*) FROM silver.crm_prd_info
GROUP BY 1
HAVING count(*) > 1 or prd_id is null

-- *** check for product cost null or negativ value

select * from silver.crm_prd_info
where prd_cost is null or prd_cost < 0.00::money


-- *** check if product end date smaller than product start date

select * from silver.crm_prd_info
where prd_end_dt < prd_start_dt 


-- 1.3 sales information

-- *** check for product key not existing in product table ***

select * from silver.crm_sales_details 
where sls_prd_key not in (select prd_key from silver.crm_prd_info)

-- *** check for customer id not existing in customer table ***

select * from silver.crm_sales_details 
where sls_cust_id not in (select cst_id from silver.crm_cust_info)

-- *** order date must be less than ship date and due date ***

select * from silver.crm_sales_details 
where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt

-- *** sales price must be equal to unity price * quantity

select * from silver.crm_sales_details 
where sls_sales != sls_quantity * sls_price 


-- 2 ERP

-- 2.1 customer information

-- *** check for duplicate or null value for customer id ***

select cid, count(*) from silver.erp_cust_az12 
group by 1
having count(*)>1 or cid is null

-- *** check for birth date higher than current date ***

select * from silver.erp_cust_az12 
where bdate > current_date 

-- *** gender values must be in ('Male','Female','N/A')

select * from silver.erp_cust_az12
where gen not in ('Male','Female','N/A')

-- 2.2 customer location

-- *** customer id must exists in silver.crm_cust_info table

select * from silver.erp_loc_a101 
where cid not in (select cst_id::varchar from silver.crm_cust_info)

-- 2.3 category information

-- *** id must exists in crm_prd_info table ***

select * from silver.erp_px_cat_g1v2
where id not in (select cat_id from silver.crm_prd_info)

-- *** maintenance values must be in ('yes','no') ***

select * from silver.erp_px_cat_g1v2
where maintenance not in('Yes','No')