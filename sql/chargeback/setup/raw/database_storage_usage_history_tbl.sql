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
create or replace transient table database_storage_usage_history_stg
(
     organization_name              varchar( 250 )      not null
    ,account_name                   varchar( 250 )      not null
    ,region_name                    varchar( 250 )      not null
    ,usage_date                     date                not null
    ,database_id                    number              not null
    ,database_name                  varchar( 250 )      not null
    ,deleted                        timestamp_ltz           null
    ,average_database_bytes         float               not null
    ,average_failsafe_bytes         float               not null
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
create or replace table database_storage_usage_history
(
     dw_event_shk                   binary( 20 )        not null
    --
    ,organization_name              varchar( 250 )      not null
    ,account_name                   varchar( 250 )      not null
    ,region_name                    varchar( 250 )      not null
    ,usage_date                     date                not null
    ,database_id                    number              not null
    ,database_name                  varchar( 250 )      not null
    ,deleted                        timestamp_ltz           null
    ,average_database_bytes         float               not null
    ,average_failsafe_bytes         float               not null
    --
    ,dw_file_name                   varchar( 250 )      not null
    ,dw_file_row_no                 number              not null
    ,dw_load_ts                     timestamp_ltz       not null
)
data_retention_time_in_days = 1
copy grants
;

