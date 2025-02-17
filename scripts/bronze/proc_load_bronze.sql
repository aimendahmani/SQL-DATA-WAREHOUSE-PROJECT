create or replace procedure bronze.load_bronze()
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
values('bronze.load_bronze', 'start',batch_start_time,null,null,null,null);

v_start_time := clock_timestamp();

insert into log.stepslog(procedure_name,step,start_time,end_time,elapsed_time,error_code,error_message)
values('bronze.load_bronze', 'insert into bronze.crm_cust_info',v_start_time,null,null,null,null);

truncate bronze.crm_cust_info ;
copy bronze.crm_cust_info
from 'C:\Users\Aimen.DAHMANI\Documents\developpements\real world project datawarehouse\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
delimiter ','
csv header;
v_end_time := clock_timestamp();
update log.stepslog 
set end_time = v_end_time,
	elapsed_time = extract(epoch from (v_end_time-v_start_time))
where procedure_name ='bronze.load_bronze'
and step = 'insert into bronze.crm_cust_info'
and end_time is null;


v_start_time := clock_timestamp();

insert into log.stepslog(procedure_name,step,start_time,end_time,elapsed_time,error_code,error_message)
values('bronze.load_bronze', 'insert into bronze.crm_prd_info',v_start_time,null,null,null,null);

truncate bronze.crm_prd_info; 
copy bronze.crm_prd_info
from 'C:\Users\Aimen.DAHMANI\Documents\developpements\real world project datawarehouse\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
delimiter ','
csv header;

v_end_time := clock_timestamp();
update log.stepslog 
set end_time = v_end_time,
	elapsed_time = extract(epoch from (v_end_time-v_start_time))
where procedure_name ='bronze.load_bronze'
and step = 'insert into bronze.crm_prd_info'
and end_time is null;

v_start_time := clock_timestamp();

insert into log.stepslog(procedure_name,step,start_time,end_time,elapsed_time,error_code,error_message)
values('bronze.load_bronze', 'insert into bronze.crm_sales_details',v_start_time,null,null,null,null);

truncate bronze.crm_sales_details;  
copy bronze.crm_sales_details
from 'C:\Users\Aimen.DAHMANI\Documents\developpements\real world project datawarehouse\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
delimiter ','
csv header;

v_end_time := clock_timestamp();
update log.stepslog 
set end_time = v_end_time,
	elapsed_time = extract(epoch from (v_end_time-v_start_time))
where procedure_name ='bronze.load_bronze'
and step = 'insert into bronze.crm_sales_details'
and end_time is null;

v_start_time := clock_timestamp();

insert into log.stepslog(procedure_name,step,start_time,end_time,elapsed_time,error_code,error_message)
values('bronze.load_bronze', 'insert into bronze.erp_cust_az12',v_start_time,null,null,null,null);

truncate bronze.erp_cust_az12; 
copy bronze.erp_cust_az12
from 'C:\Users\Aimen.DAHMANI\Documents\developpements\real world project datawarehouse\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
delimiter ','
csv header;

v_end_time := clock_timestamp();
update log.stepslog 
set end_time = v_end_time,
	elapsed_time = extract(epoch from (v_end_time-v_start_time))
where procedure_name ='bronze.load_bronze'
and step = 'insert into bronze.erp_cust_az12'
and end_time is null;

v_start_time := clock_timestamp();

insert into log.stepslog(procedure_name,step,start_time,end_time,elapsed_time,error_code,error_message)
values('bronze.load_bronze', 'insert into bronze.erp_loc_a101',v_start_time,null,null,null,null);

truncate bronze.erp_loc_a101;  
copy bronze.erp_loc_a101
from 'C:\Users\Aimen.DAHMANI\Documents\developpements\real world project datawarehouse\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
delimiter ','
csv header;

v_end_time := clock_timestamp();
update log.stepslog 
set end_time = v_end_time,
	elapsed_time = extract(epoch from (v_end_time-v_start_time))
where procedure_name ='bronze.load_bronze'
and step = 'insert into bronze.erp_loc_a101'
and end_time is null;

v_start_time := clock_timestamp();

insert into log.stepslog(procedure_name,step,start_time,end_time,elapsed_time,error_code,error_message)
values('bronze.load_bronze', 'insert into bronze.erp_px_cat_g1v2',v_start_time,null,null,null,null);

truncate bronze.erp_px_cat_g1v2;  
copy bronze.erp_px_cat_g1v2
from 'C:\Users\Aimen.DAHMANI\Documents\developpements\real world project datawarehouse\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
delimiter ','
csv header;

v_end_time := clock_timestamp();
update log.stepslog 
set end_time = v_end_time,
	elapsed_time = extract(epoch from (v_end_time-v_start_time))
where procedure_name ='bronze.load_bronze'
and step = 'insert into bronze.erp_px_cat_g1v2'
and end_time is null;

batch_end_time := clock_timestamp();
update log.stepslog
set end_time = batch_end_time,
	elapsed_time = extract(epoch from (batch_end_time-batch_start_time))
where procedure_name = 'bronze.load_bronze'
and step = 'start'
and end_time is null;
end; 
$$
call bronze.load_bronze()
