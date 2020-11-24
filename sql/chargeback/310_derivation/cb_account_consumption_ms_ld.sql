 
set (l_start_dt, l_end_dt ) = (select date_trunc( month, start_dt ), date_trunc( month, dateadd( month, 1, end_dt ) ) from table( {environment}_plt_common_db.util.dw_delta_date_range_f( 'all' ) ));

-- wrap delete and insert within a transaction so a failure with the insert doesn't leave the table without data for the delta range
begin;

    --------------------------------------------------------------------
    -- delete periods within the delta range
    --
    delete from
        {environment}_plt_il_db.main.cb_account_consumption_ms
    where
            event_month_dt >= $l_start_dt
        and event_month_dt  < $l_end_dt
    ;
    
    --------------------------------------------------------------------
    -- load delta
    --
    insert into 
        {environment}_plt_il_db.main.cb_account_consumption_ms
    select
         carl.dw_account_shk
        ,crcm.dw_event_date_sid
        ,crcm.event_month_dt
        --
        ,sum( crcm.compute_credit_cnt )    as compute_credit_cnt
        ,sum( crcm.compute_cost_amt   )    as compute_cost_amt
        ,sum( crcm.storage_byte_cnt   )    as storage_byte_cnt
        ,sum( crcm.storage_tb_cnt     )    as storage_tb_cnt
        ,sum( crcm.storage_cost_amt   )    as storage_cost_amt
        ,sum( crcm.data_xfer_byte_cnt )    as data_xfer_byte_cnt
        ,sum( crcm.data_xfer_tb_cnt   )    as data_xfer_tb_cnt
        ,sum( crcm.data_xfer_cost_amt )    as data_xfer_cost_amt
        ,sum( crcm.total_cost_amt     )    as total_cost_amt
        --
        ,current_timestamp()               as dw_load_ts
    from 
        {environment}_plt_il_db.main.cb_resource_consumption_ms crcm
        join {environment}_plt_il_db.main.cb_account_resource_l carl on
            carl.dw_resource_shk = crcm.dw_resource_shk
    where
        not exists 
        (
            select 1 from {environment}_plt_il_db.main.cb_account_consumption_ms where event_month_dt >= $l_start_dt and event_month_dt < $l_end_dt
        )
        -- crcm
        and crcm.event_month_dt >= $l_start_dt
        and crcm.event_month_dt  < $l_end_dt
    group by
        1,2,3
    order by
        2,1
    ;

commit;