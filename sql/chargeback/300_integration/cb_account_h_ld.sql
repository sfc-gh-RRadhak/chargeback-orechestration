

set (l_start_dt, l_end_dt ) = (select start_dt, end_dt + 1 from table( {environment}_plt_common_db.util.dw_delta_date_range_f( 'all' ) ));

insert into 
    {environment}_plt_il_db.main.cb_account_h
with l_account as
(
    select distinct 
         wmh.organization_name
        ,wmh.account_name
        ,wmh.region_name
    from   
        {environment}_plt_rl_snowflake_db.account_usage.warehouse_metering_history wmh
    where
        -- wmh
            wmh.end_time        >= $l_start_dt
        and wmh.end_time         < $l_end_dt
    union
    select distinct 
         dsuh.organization_name
        ,dsuh.account_name
        ,dsuh.region_name
    from   
        {environment}_plt_rl_snowflake_db.account_usage.database_storage_usage_history dsuh
    where
        -- dsuh
            dsuh.usage_date     >= $l_start_dt
        and dsuh.usage_date      < $l_end_dt
    union
    select distinct 
         ach.organization_name
        ,ach.account_name
        ,ach.region_name
    from   
        {environment}_plt_rl_snowflake_db.account_usage.automatic_clustering_history ach
    where
        -- ach
            ach.end_time        >= $l_start_dt
        and ach.end_time         < $l_end_dt
    union
    select distinct 
         dth.organization_name
        ,dth.account_name
        ,dth.region_name
    from   
        {environment}_plt_rl_snowflake_db.account_usage.data_transfer_history dth
    where
        -- dth
            dth.end_time        >= $l_start_dt
        and dth.end_time         < $l_end_dt
)
,l_account_shk as
(
    select 
        -- generate hash key
         sha1_binary( concat( la.organization_name
                             ,'|', la.account_name
                             ,'|', la.region_name
                            )
                    )                   as dw_account_shk
        --
        ,la.organization_name 
        ,la.account_name    
        ,la.region_name
    from 
        l_account la
)
select
     las.dw_account_shk
    --
    ,las.organization_name 
    ,las.account_name    
    ,las.region_name
    --
    ,current_timestamp()            as dw_load_ts
    ,current_timestamp()            as dw_update_ts
from
    l_account_shk las
where
    las.dw_account_shk not in
    (
        select dw_account_shk from {environment}_plt_il_db.main.cb_account_h
    )
order by
    2,3,4
;
