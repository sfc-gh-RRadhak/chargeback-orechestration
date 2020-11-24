insert overwrite into {environment}_plt_il_db.main.sf_compute_cost_s
select
     cah.dw_account_shk
    ,cst.active_dt
    ,cst.cost_amt
    ,cst.inactive_dt
    ,current_timestamp()        as dw_load_ts
from
    {environment}_plt_rl_snowflake_db.account_usage.sf_compute_cost_lkp cst
    cross join {environment}_plt_il_db.main.cb_account_h cah
order by
    2, 1 


