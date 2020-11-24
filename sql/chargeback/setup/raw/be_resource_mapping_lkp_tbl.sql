--------------------------------------------------------------------
--  Purpose: create psa tables
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  dd/mm/yy
--------------------------------------------------------------------
use role {role};
use database {environment}_plt_rl_snowflake_db;
use schema account_usage;

--
-- permanent table with retention days
--
create or replace table be_resource_mapping_lkp
(
     match_pattern                  varchar( 100 )
    ,priority_no                    number
    ,tag_json                       variant
    --
    ,dw_load_ts                     timestamp_ltz       not null
)
data_retention_time_in_days = 1
copy grants
;
