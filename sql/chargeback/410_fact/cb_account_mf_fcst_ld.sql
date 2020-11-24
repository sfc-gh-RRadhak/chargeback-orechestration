--
set (l_start_dt, l_end_dt) = (select date_trunc( month, current_date() ), date_trunc( month, dateadd( month, 12, current_date() ) ));

-- wrap delete and insert within a transaction so a failure with the insert doesn't leave the table without data for the delta range
begin;

    --------------------------------------------------------------------
    -- delete periods within the delta range
    --
    delete from 
        {environment}_plt_pl_db.main.cb_account_mf
    where
            event_month_dt >= $l_start_dt
        and event_month_dt  < $l_end_dt
    ;
    
    --------------------------------------------------------------------
    -- load delta
    --
    insert into 
        {environment}_plt_pl_db.main.cb_account_mf 
    select
         dw_account_shk
        ,dw_event_date_sid
        --
        ,event_month_dt
        ,max( compute_credit_cnt  )         as compute_credit_cnt  
        ,max( compute_cost_amt    )         as compute_cost_amt   
        ,max( storage_byte_cnt    )         as storage_byte_cnt   
        ,max( storage_cost_amt    )         as storage_cost_amt   
        ,max( data_xfer_byte_cnt  )         as data_xfer_byte_cnt 
        ,max( data_xfer_cost_amt  )         as data_xfer_cost_amt 
        ,max( total_cost_amt      )         as total_cost_amt     
        ,max( fcst_total_cost_amt )         as fcst_total_cost_amt
        ,current_timestamp()                as dw_load_ts
        ,current_timestamp()                as dw_update_ts
    from
        (
        -- actuals
        select 
             cacm.dw_account_shk
            ,cacm.dw_event_date_sid
            --
            ,cacm.event_month_dt
            ,cacm.compute_credit_cnt 
            ,cacm.compute_cost_amt
            ,cacm.storage_byte_cnt
            ,cacm.storage_cost_amt
            ,cacm.data_xfer_byte_cnt
            ,cacm.data_xfer_cost_amt
            ,cacm.total_cost_amt
            ,0                                  as fcst_total_cost_amt
        from 
            {environment}_plt_il_db.main.cb_account_consumption_ms cacm
        where
                cacm.event_month_dt >= $l_start_dt
            and cacm.event_month_dt  < $l_end_dt
        union all
        -- forecast
        select 
             cacfm.dw_account_shk
            ,cacfm.dw_event_date_sid
            --
            ,cacfm.event_month_dt
            ,0                                  as compute_credit_cnt 
            ,0                                  as compute_cost_amt
            ,0                                  as storage_byte_cnt
            ,0                                  as storage_cost_amt
            ,0                                  as data_xfer_byte_cnt
            ,0                                  as data_xfer_cost_amt
            ,0                                  as total_cost_amt
            ,cacfm.fcst_total_cost_amt
        from 
            {environment}_plt_il_db.main.cb_account_consumption_fcst_ms cacfm
        where
                cacfm.event_month_dt >= $l_start_dt
            and cacfm.event_month_dt  < $l_end_dt
        )
    group by
        2,1,3
    order by
        2,1
    ;

commit;