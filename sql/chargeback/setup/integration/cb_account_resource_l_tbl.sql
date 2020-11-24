use role {role};
use database {environment}_plt_il_db;
use schema main;

-- permanent table with retention days
-----
create or replace table cb_account_resource_l
(
     dw_account_shk                 binary( 20 )        not null
    ,dw_resource_shk                binary( 20 )        not null
    --
    ,dw_load_ts                     timestamp_ltz       not null
    ,dw_update_ts                   timestamp_ltz       not null
)
data_retention_time_in_days = 1
copy grants
; 