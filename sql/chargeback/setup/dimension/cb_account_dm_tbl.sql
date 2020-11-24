use role     {role};
use database {environment}_plt_pl_db;
use schema   main;
 
create or replace table cb_account_dm
(
     dw_account_shk                 binary( 20 )
    --
    ,be_name                        varchar( 250 )
    ,organization_name              varchar( 250 )
    ,account_name                   varchar( 250 )
    ,region_name                    varchar( 250 )
    --
    ,dw_load_ts                     timestamp_ltz       not null
)
data_retention_time_in_days = 1
copy grants
;

