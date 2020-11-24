
--------------------------------------------------------------------
-- date range to process
--
set (l_base_dt, l_start_dt, l_end_dt, l_month_cnt) = 
(
    select
         date_trunc( month, current_date() )        as base_dt
        ,dateadd( month, -12, base_dt )             as start_dt
        ,dateadd( month,  12, base_dt )             as end_dt
        ,datediff( month, start_dt, end_dt )        as month_cnt
);

--------------------------------------------------------------------
-- populate forecasting input table with all prior months to model
-- and future months to forecast
--
create or replace temporary table {environment}_plt_il_db.main.fcst_period_in as
with l_range_month as
(
    select
         {environment}_plt_common_db.util.date_sid_f( event_month_dt )    as dw_event_date_sid
        ,event_month_dt
        ,case
            when event_month_dt >= $l_base_dt then 1
            else 0
         end        as forecast_period_bt
    from
        (
        select
            dateadd( month, seq1(), $l_start_dt ) as event_month_dt 
        from
            table( generator( rowcount => $l_month_cnt ) )
        )
)
,l_account as
(
    -- get distinct set of accounts being forecasted
    select
         dw_account_shk
        ,min( event_month_dt ) as first_month_dt
    from
        dev_plt_il_db.main.cb_account_consumption_ms
    group by
        1
)
,l_account_month as
(
    -- fill in all prior and future months for each account
    select
         la.dw_account_shk
        ,lrm.dw_event_date_sid
        ,lrm.event_month_dt
        ,lrm.forecast_period_bt
    from
        l_account la
        cross join l_range_month lrm
    where
        lrm.event_month_dt >= la.first_month_dt
)
select
     lam.dw_account_shk                         as partition_key
    ,lam.event_month_dt                         as period_dt
    ,date_part( year, lam.event_month_dt )      as year_no
    ,date_part( month, lam.event_month_dt )     as period_no
    ,case lam.forecast_period_bt
        when 1 
        then null
        else ifnull( cacm.total_cost_amt, 0 )                    
    end                                         as measure_no
    --
    ,lam.dw_account_shk
    ,lam.dw_event_date_sid
    ,lam.event_month_dt
    ,ifnull( cacm.total_cost_amt, 0 )           as total_cost_amt
from
    l_account_month lam
    left join {environment}_plt_il_db.main.cb_account_consumption_ms cacm on
            cacm.dw_account_shk = lam.dw_account_shk
        and cacm.event_month_dt = lam.event_month_dt
order by
     lam.dw_account_shk
    ,lam.event_month_dt
;

--------------------------------------------------------------------
-- forecast current and future months
-- 
call {environment}_plt_common_db.util.fcst_linear_trend_sp( '{environment}_plt_il_db.main.fcst_period_in', '{environment}_plt_il_db.main.fcst_period_out' );

-- wrap delete and insert within a transaction so a failure with the insert doesn't leave the table without data for the delta range
begin;

    --------------------------------------------------------------------
    -- delete periods within the delta range
    --
    delete from
        {environment}_plt_il_db.main.cb_account_consumption_fcst_ms
    where
            event_month_dt >= $l_start_dt
        and event_month_dt  < $l_end_dt
    ;
    
    --------------------------------------------------------------------
    -- load delta
    --
    insert into 
        {environment}_plt_il_db.main.cb_account_consumption_fcst_ms
    select
         fpi.dw_account_shk
        ,fpi.dw_event_date_sid
        --
        ,fpi.event_month_dt
        ,fpi.total_cost_amt
        ,fpo.fcst_measure_no    as fcst_total_cost_amt
        --
        ,current_timestamp()    as dw_load_ts
    from
        {environment}_plt_il_db.main.fcst_period_in fpi
        join {environment}_plt_il_db.main.fcst_period_out fpo on
                fpo.partition_key = fpi.partition_key
            and fpo.period_dt     = fpi.period_dt
    where
        not exists 
        (
            select 1 from {environment}_plt_il_db.main.cb_account_consumption_fcst_ms where event_month_dt >= $l_start_dt and event_month_dt < $l_end_dt
        )
    ;

commit;
