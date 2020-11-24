 
use role     {role};
use database {environment}_plt_pl_db;
use schema   main;
 
create or replace table cb_resource_dm
(
     dw_resource_shk                binary( 20 )
    --
    ,be_name                        varchar( 250 )
    ,organization_name              varchar( 250 )
    ,account_name                   varchar( 250 )
    ,region_name                    varchar( 250 )
    ,resource_name                  varchar( 250 )
    ,resource_type_cd               varchar( 250 )
    ,team_name                      varchar( 250 )
    ,env_type_cd                    varchar( 250 )
    ,gl_account_str                 varchar( 250 )
    --
    ,dw_load_ts                     timestamp_ltz       not null
)
data_retention_time_in_days = 1
copy grants
;


