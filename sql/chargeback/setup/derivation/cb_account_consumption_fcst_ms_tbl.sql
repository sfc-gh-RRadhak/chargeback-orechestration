--------------------------------------------------------------------
--  Purpose: create psa tables
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  dd/mm/yy
--------------------------------------------------------------------
use role     {role};
use database {environment}_plt_il_db;
use schema   main;

--
-- permanent table with retention days
--
create or replace table cb_account_consumption_fcst_ms
(
     dw_account_shk                 binary( 20 )
    ,dw_event_date_sid              number
    --
    ,event_month_dt                 date
    ,total_cost_amt                 float
    ,fcst_total_cost_amt            float
    --
    ,dw_load_ts                     timestamp_ltz       not null
)
data_retention_time_in_days = 1
copy grants
;