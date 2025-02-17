drop table if exists log.stepslog;

create table log.stepslog( 

id serial primary key,
procedure_name varchar(50),
step varchar(50),
start_time timestamp,
end_time timestamp,
elapsed_time text,
error_code int,
error_message text

);
