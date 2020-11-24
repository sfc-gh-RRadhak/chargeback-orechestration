
use role {role};
use database {environment}_plt_rl_snowflake_db;
use schema account_usage;

create or replace table cb_service_type_lkp
(
     dw_service_type_shk            binary( 20 )        not null
    ,dw_hash_diff                   binary( 20 )        not null
    ,service_type_cd                varchar( 50 )       not null
    ,service_type_name              varchar( 250 )      not null
    ,service_type_group_name        varchar( 250 )      not null
    ,active_dt                      date                not null
    ,inactive_dt                    date                not null
    --
    ,dw_load_ts                     timestamp_ltz       not null
    ,dw_update_ts                   timestamp_ltz       not null
)
data_retention_time_in_days = 1
copy grants
;