 
use role {role};
use database {environment}_plt_rl_snowflake_db;
use schema account_usage;

--
-- permanent table with retention days
--
create or replace table sf_compute_cost_lkp
(
     cost_amt                       float               not null
    ,active_dt                      date                not null
    ,inactive_dt                    date                not null
    --
    ,dw_load_ts                     timestamp_ltz       not null
)
data_retention_time_in_days = 1
copy grants
;
