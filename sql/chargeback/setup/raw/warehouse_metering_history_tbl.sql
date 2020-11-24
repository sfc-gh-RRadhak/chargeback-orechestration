--------------------------------------------------------------------
--  Purpose: create psa tables
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  dd/mm/yy
--------------------------------------------------------------------
use role sysadmin;

use database {environment}_plt_rl_snowflake_db;
use schema account_usage;


--
-- transient staging table with no retention days
--
create or replace transient table warehouse_metering_history_stg
(
     organization_name              varchar( 250 )      not null
    ,account_name                   varchar( 250 )      not null
    ,region_name                    varchar( 250 )      not null
    ,start_time                     timestamp_ltz       not null
    ,end_time                       timestamp_ltz       not null
    ,warehouse_id                   number              not null
    ,warehouse_name                 varchar( 250 )      not null
    ,credits_used                   float               not null
    ,credits_used_compute           float               not null
    ,credits_used_cloud_services    float               not null
    --
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
create or replace table warehouse_metering_history
(
     dw_event_shk                   binary( 20 )        not null
    --
    ,organization_name              varchar( 250 )      not null
    ,account_name                   varchar( 250 )      not null
    ,region_name                    varchar( 250 )      not null
    ,start_time                     timestamp_ltz       not null
    ,end_time                       timestamp_ltz       not null
    ,warehouse_id                   number              not null
    ,warehouse_name                 varchar( 250 )      not null
    ,credits_used                   float               not null
    ,credits_used_compute           float               not null
    ,credits_used_cloud_services    float               not null
    --
    ,dw_file_name                   varchar( 250 )      not null
    ,dw_file_row_no                 number              not null
    ,dw_load_ts                     timestamp_ltz       not null
)
data_retention_time_in_days = 1
copy grants
;

