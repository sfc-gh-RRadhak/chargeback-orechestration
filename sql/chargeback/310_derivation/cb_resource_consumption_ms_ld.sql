set (l_start_dt, l_end_dt ) = (select date_trunc( month, start_dt ), date_trunc( month, dateadd( month, 1, end_dt ) ) from table( {environment}_plt_common_db.util.dw_delta_date_range_f( 'all' ) ));

begin;

    --------------------------------------------------------------------
    -- delete periods within the delta range
    --
    delete from
        {environment}_plt_il_db.main.cb_resource_consumption_ms
    where
            event_month_dt >= $l_start_dt
        and event_month_dt  < $l_end_dt
    ;
    
    --------------------------------------------------------------------
    -- load delta
    --
    insert into 
        {environment}_plt_il_db.main.cb_resource_consumption_ms
    with l_resource_consumption as
    (
        --
        -- warehouse metering
        --
        select
             cah.dw_account_shk
            ,csh.dw_resource_shk
            ,cstl.dw_service_type_shk
            ,{environment}_plt_common_db.util.date_sid_f( date_trunc( month, wmh.end_time ) )    as dw_event_date_sid
            ,date_trunc( month, wmh.end_time )                  as event_month_dt
            --
            ,sum( wmh.credits_used )                            as compute_credit_cnt
            ,0                                                  as storage_byte_cnt
            ,0                                                  as data_xfer_byte_cnt
        from 
            {environment}_plt_rl_snowflake_db.account_usage.warehouse_metering_history wmh
            join {environment}_plt_il_db.main.cb_resource_h csh on
                    csh.organization_name = wmh.organization_name
                and csh.account_name      = wmh.account_name
                and csh.region_name       = wmh.region_name
                and csh.resource_name     = wmh.warehouse_name
            join {environment}_plt_il_db.main.cb_account_h cah on
                    cah.organization_name = wmh.organization_name
                and cah.account_name      = wmh.account_name
                and cah.region_name       = wmh.region_name
            cross join {environment}_plt_rl_snowflake_db.account_usage.cb_service_type_lkp cstl
        where
            -- wmh
                wmh.end_time        >= $l_start_dt
            and wmh.end_time         < $l_end_dt
            -- csh
            and csh.resource_type_cd = 'warehouse'
            -- cstl
            and cstl.service_type_cd = 'WAREHOUSE_METERING'
        group by
            1,2,3,4,5
        union all
        --
        -- automatic clustering
        --
        select 
             cah.dw_account_shk
            ,csh.dw_resource_shk
            ,cstl.dw_service_type_shk
            ,{environment}_plt_common_db.util.date_sid_f( date_trunc( month, ach.end_time ) )    as dw_event_date_sid
            ,date_trunc( month, ach.end_time )                  as event_month_dt
            --
            ,sum( ach.credits_used )                            as compute_credit_cnt
            ,0                                                  as storage_byte_cnt
            ,0                                                  as data_xfer_byte_cnt
        from 
            {environment}_plt_rl_snowflake_db.account_usage.automatic_clustering_history ach
            join {environment}_plt_il_db.main.cb_resource_h csh on
                    csh.organization_name = ach.organization_name
                and csh.account_name      = ach.account_name
                and csh.region_name       = ach.region_name
                and csh.resource_name     = ach.database_name
            join {environment}_plt_il_db.main.cb_account_h cah on
                    cah.organization_name = ach.organization_name
                and cah.account_name      = ach.account_name
                and cah.region_name       = ach.region_name
            cross join {environment}_plt_rl_snowflake_db.account_usage.cb_service_type_lkp cstl
        where
            -- ach
                ach.end_time        >= $l_start_dt
            and ach.end_time         < $l_end_dt
            -- csh
            and csh.resource_type_cd = 'database'
            -- cstl
            and cstl.service_type_cd = 'AUTOMATIC_CLUSTERING'
        group by
            1,2,3,4,5
        union all
        --
        -- database storage
        --
        select 
             cah.dw_account_shk
            ,csh.dw_resource_shk
            ,cstl.dw_service_type_shk
            ,{environment}_plt_common_db.util.date_sid_f( date_trunc( month, dsuh.usage_date ) )     as dw_event_date_sid
            ,date_trunc( month, dsuh.usage_date )                   as event_month_dt
            --
            ,0                                                      as compute_credit_cnt
            ,avg( dsuh.average_database_bytes )                     as storage_byte_cnt
            ,0                                                      as data_xfer_byte_cnt
        from 
            {environment}_plt_rl_snowflake_db.account_usage.database_storage_usage_history dsuh
            join {environment}_plt_il_db.main.cb_resource_h csh on
                    csh.organization_name = dsuh.organization_name
                and csh.account_name      = dsuh.account_name
                and csh.region_name       = dsuh.region_name
                and csh.resource_name     = dsuh.database_name
            join {environment}_plt_il_db.main.cb_account_h cah on
                    cah.organization_name = dsuh.organization_name
                and cah.account_name      = dsuh.account_name
                and cah.region_name       = dsuh.region_name
            cross join {environment}_plt_rl_snowflake_db.account_usage.cb_service_type_lkp cstl
        where
            -- dsuh
                dsuh.usage_date     >= $l_start_dt
            and dsuh.usage_date      < $l_end_dt
            -- csh
            and csh.resource_type_cd = 'database'
            -- cstl
            and cstl.service_type_cd = 'STORAGE_DATABASE'
        group by
            1,2,3,4,5
        union all
        --
        -- data transfer
        --
        select 
             cah.dw_account_shk
            ,csh.dw_resource_shk
            ,cstl.dw_service_type_shk
            ,{environment}_plt_common_db.util.date_sid_f( date_trunc( month, dth.end_time ) )        as dw_event_date_sid
            ,date_trunc( month, dth.end_time )                      as event_month_dt
            --                                                      
            ,0                                                      as compute_credit_cnt
            ,0                                                      as storage_byte_cnt
            ,sum( dth.bytes_transferred )                           as data_xfer_byte_cnt
        from 
            {environment}_plt_rl_snowflake_db.account_usage.data_transfer_history dth
            join {environment}_plt_il_db.main.cb_resource_h csh on
                    csh.organization_name = dth.organization_name
                and csh.account_name      = dth.account_name
                and csh.region_name       = dth.region_name
                and csh.resource_name     = concat( ifnull( dth.target_cloud, '?' ), '.', ifnull( dth.target_region, '?' ) )
            join {environment}_plt_il_db.main.cb_account_h cah on
                    cah.organization_name = dth.organization_name
                and cah.account_name      = dth.account_name
                and cah.region_name       = dth.region_name
            cross join {environment}_plt_rl_snowflake_db.account_usage.cb_service_type_lkp cstl
        where
            -- dth
                dth.end_time        >= $l_start_dt
            and dth.end_time         < $l_end_dt
            -- csh
            and csh.resource_type_cd = 'cloudregion'
            -- cstl
            and cstl.service_type_cd = 'DATA_TRANSFER'
        group by
            1,2,3,4,5
    )
    select
         lrc.dw_account_shk
        ,lrc.dw_resource_shk
        ,lrc.dw_service_type_shk
        ,lrc.dw_event_date_sid
        ,lrc.event_month_dt
        --         
        ,lrc.compute_credit_cnt
        ,lrc.compute_credit_cnt * {environment}_plt_common_db.util.sf_compute_cost_f( lrc.dw_account_shk, lrc.event_month_dt )
                                                                as compute_cost_amt
        ,lrc.storage_byte_cnt
        ,lrc.storage_byte_cnt / pow( 1024, 4 )                  as storage_tb_cnt
        ,(lrc.storage_byte_cnt / pow( 1024, 4 )) * {environment}_plt_common_db.util.sf_storage_cost_f( lrc.dw_account_shk, lrc.event_month_dt )
                                                                as storage_cost_amt
        ,lrc.data_xfer_byte_cnt
        ,lrc.data_xfer_byte_cnt / pow( 1024, 4 )                as data_xfer_tb_cnt
        ,(lrc.data_xfer_byte_cnt / pow( 1024, 4 )) * {environment}_plt_common_db.util.sf_data_xfer_cost_f( lrc.dw_account_shk, lrc.event_month_dt ) 
                                                                as data_xfer_cost_amt
        ,compute_cost_amt 
         + storage_cost_amt 
         + data_xfer_cost_amt                                   as total_cost_amt                                             
        --
        ,current_timestamp()                                    as dw_load_ts
    from
        l_resource_consumption lrc
    where
        not exists 
        (
            select 1 from {environment}_plt_il_db.main.cb_resource_consumption_ms where event_month_dt >= $l_start_dt and event_month_dt < $l_end_dt
        )
    order by
        3
    ;

commit;
