
-- CRM SOURCE TABLES


drop table if exists silver.crm_cust_info;

create table silver.crm_cust_info (

cst_id INT,
cst_key varchar(50),
cst_firstname varchar(50),
cst_lastname varchar(50),
cst_marital_status varchar(50),
cst_gndr varchar(50),
cst_create_date date,
dwh_create_date timestamp default now()

);

drop table if exists silver.crm_prd_info;

create table silver.crm_prd_info (

prd_id INT,
cat_id varchar(50),
prd_key varchar(50),
prd_nm varchar(50),
prd_cost money,
prd_line varchar(50),
prd_start_dt date,
prd_end_dt date,
dwh_create_date timestamp default now()
);

drop table if exists silver.crm_sales_details;

create table silver.crm_sales_details ( 

sls_ord_num varchar(50),
sls_prd_key varchar(50),
sls_cust_id int,
sls_order_dt int,
sls_ship_dt int,
sls_due_dt int,
sls_sales int,
sls_quantity int,
sls_price money,
dwh_create_date timestamp default now()
);

--ERP SOURCE TABLES

drop table if exists silver.erp_cust_az12;

create table silver.erp_cust_az12(

CID varchar(50),
BDATE date,
GEN varchar(10),
dwh_create_date timestamp default now()

);

drop table if exists silver.erp_LOC_A101;

create table silver.erp_LOC_A101(

CID varchar(50),
CNTRY varchar(50),
dwh_create_date timestamp default now()

);

drop table if exists silver.erp_px_cat_g1v2;

create table silver.erp_px_cat_g1v2(

ID varchar(50),
CAT varchar(50),
SUBCAT varchar(50),
MAINTENANCE varchar(50),
dwh_create_date timestamp default now()

);
