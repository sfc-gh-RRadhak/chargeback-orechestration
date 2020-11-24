--------------------------------------------------------------------
--  Purpose: load lookup table with an insert-only pattern since the 
--           pk has the potential of changing.
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  dd/mm/yy
--------------------------------------------------------------------
 

-- cost rows for each account and time period
insert overwrite into {environment}_plt_rl_snowflake_db.account_usage.sf_data_xfer_cost_lkp
select
     cst.cost_amt
    ,cst.active_dt
    ,cst.inactive_dt
    --
    ,current_timestamp()        as dw_load_ts
from
    ( values
          ( 90.00, '2018-01-01', '2019-01-01')
         ,( 87.00, '2019-01-01', '2020-12-01')
         ,( 86.00, '2020-12-01', '2888-01-01')
    ) cst( cost_amt, active_dt, inactive_dt )
order by
    cst.active_dt
;
 

