--------------------------------------------------------------------
--  Purpose: create psa tables
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  dd/mm/yy
--------------------------------------------------------------------
use role     {role};
use database {environment}_plt_common_db;
use schema   util;

create or replace function sf_compute_cost_f
( 
     p_dw_account_shk   binary
    ,p_date             date 
)
returns float
as
$$
    select 
        max( cost_amt ) 
    from 
        {environment}_plt_il_db.main.sf_compute_cost_s
    where
            dw_account_shk    = p_dw_account_shk
        and p_date           >= active_dt
        and p_date            < inactive_dt
$$
;

select
     cah.*
    ,sf_compute_cost_f( cah.dw_account_shk, dateadd( month, -24, current_date() ) ) as pre_cost_amt
    ,sf_compute_cost_f( cah.dw_account_shk, dateadd( month, 0, current_date() ) )   as now_cost_amt
    ,sf_compute_cost_f( cah.dw_account_shk, dateadd( month, 13, current_date() ) )  as next_cost_amt
from
    {environment}_plt_il_db.main.cb_account_h cah
;
 

