--------------------------------------------------------------------
--  Purpose: 
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  dd/mm/yy
--------------------------------------------------------------------

insert overwrite into {environment}_plt_common_db.util.dw_delta_date
with l_delta_date as
(
    select distinct 
        to_date( end_time )    as event_dt
    from   
        {environment}_plt_rl_snowflake_db.account_usage.warehouse_metering_history_stg
    union
    select distinct 
        to_date( usage_date )  as event_dt
    from   
        {environment}_plt_rl_snowflake_db.account_usage.database_storage_usage_history_stg
    union
    select distinct 
        to_date( end_time )    as event_dt
    from   
        {environment}_plt_rl_snowflake_db.account_usage.automatic_clustering_history_stg
    union
    select distinct 
        to_date( end_time )    as event_dt
    from   
        {environment}_plt_rl_snowflake_db.account_usage.data_transfer_history_stg
)
select 
     event_dt 
    ,current_timestamp()            as dw_load_ts
from 
    l_delta_date
order by
    1
;
