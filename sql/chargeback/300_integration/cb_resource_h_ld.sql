set (l_start_dt, l_end_dt ) = (select start_dt, end_dt + 1 from table( {environment}_plt_common_db.util.dw_delta_date_range_f( 'all' ) ));

insert into 
    {environment}_plt_il_db.main.cb_resource_h
with l_resource as
(
    select distinct 
         wmh.organization_name
        ,wmh.account_name
        ,wmh.region_name
        ,wmh.warehouse_name         as resource_name
        ,'warehouse'                as resource_type_cd
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
        ,dsuh.database_name         as resource_name
        ,'database'                 as resource_type_cd
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
        ,ach.database_name         as resource_name
        ,'database'                as resource_type_cd
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
        ,concat( ifnull( dth.target_cloud, '?' ), '.', ifnull( dth.target_region, '?' ) )   as resource_name
        ,'cloudregion'                                                                      as resource_type_cd
    from   
        {environment}_plt_rl_snowflake_db.account_usage.data_transfer_history dth
    where
        -- dth
            dth.end_time        >= $l_start_dt
        and dth.end_time         < $l_end_dt
)
,l_resource_shk as
(
    select 
        -- generate hash key
         sha1_binary( concat( lr.organization_name
                             ,'|', lr.account_name
                             ,'|', lr.region_name
                             ,'|', lr.resource_name
                             ,'|', lr.resource_type_cd
                            )
                    )                   as dw_resource_shk
        --
        ,lr.organization_name 
        ,lr.account_name    
        ,lr.region_name
        ,lr.resource_name     
        ,lr.resource_type_cd  
    from 
        l_resource lr
)
select
     lrs.dw_resource_shk
    --
    ,lrs.organization_name 
    ,lrs.account_name    
    ,lrs.region_name
    ,lrs.resource_name     
    ,lrs.resource_type_cd  
    --
    ,current_timestamp()            as dw_load_ts
    ,current_timestamp()            as dw_update_ts
from
    l_resource_shk lrs
where
    lrs.dw_resource_shk not in
    (
        select dw_resource_shk from {environment}_plt_il_db.main.cb_resource_h
    )
order by
    2,3,4,5
;