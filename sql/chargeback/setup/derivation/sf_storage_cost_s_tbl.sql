
use role     {role};
use database {environment}_plt_il_db;
use schema   main;
 
create or replace table sf_storage_cost_s
(
     dw_account_shk                 binary( 20 )        not null
    ,active_dt                      date                not null
    --
    ,cost_amt                       float               not null
    ,inactive_dt                    date                not null
    --
    ,dw_load_ts                     timestamp_ltz       not null
)
data_retention_time_in_days = 1
copy grants
;
