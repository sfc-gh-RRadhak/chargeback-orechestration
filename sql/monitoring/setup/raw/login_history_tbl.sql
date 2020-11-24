--------------------------------------------------------------------
--  Purpose: create psa tables
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  dd/mm/yy
--------------------------------------------------------------------
use role {role};
use database {environment}_plt_rl_snowflake_db;
use schema account_usage;


--
-- transient staging table with no retention days
--

 create or replace transient table login_history_stg (
	organization_name varchar(500)
    ,account_name varchar(500)
    ,region_name varchar(500)
    ,event_timestamp timestamp_ltz(3)
    ,event_id varchar(500)
    ,event_type varchar(500)
    ,user_name varchar(500)
    ,client_ip varchar(500)
    ,reported_client_type varchar(16777216)
    ,reported_client_version varchar(16777216)
    ,first_authentication_factor varchar(500)
    ,second_authentication_factor varchar(500)
    ,is_success varchar(3)
    ,error_code varchar(500)
    ,error_message varchar(16777216)
    ,related_event_id varchar(500)
    
    ,dw_file_name                   varchar( 250 )      not null
    ,dw_file_row_no                 number              not null
    ,dw_load_ts                     timestamp_ltz       not null
)
data_retention_time_in_days = 0
copy grants
;

--
-- permanent history table with retention days
--
Create or replace table login_history
(
     dw_event_shk   binary( 20 )    not null
   	,organization_name varchar(500)
    ,account_name varchar(500)
    ,region_name varchar(500)
    ,event_timestamp timestamp_ltz(3)
    ,event_id varchar(500)
    ,event_type varchar(500)
    ,user_name varchar(500)
    ,client_ip varchar(500)
    ,reported_client_type varchar(16777216)
    ,reported_client_version varchar(16777216)
    ,first_authentication_factor varchar(500)
    ,second_authentication_factor varchar(500)
    ,is_success varchar(3)
    ,error_code varchar(500)
    ,error_message varchar(16777216)
    ,related_event_id varchar(500)
     
    ,dw_file_name                   varchar( 250 )      not null
    ,dw_file_row_no                 number              not null
    ,dw_load_ts                     timestamp_ltz       not null
)
data_retention_time_in_days = 1
copy grants
;

