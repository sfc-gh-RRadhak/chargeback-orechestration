--------------------------------------------------------------------
--  Purpose: Internal stage for accout_usage data
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  dd/mm/yy
--------------------------------------------------------------------
use role     sysadmin;
use database {environment}_plt_common_db;
use schema   util;

create or replace stage monitoring_stg;

list @monitoring_stg;

--remove @monitoring_stg;
