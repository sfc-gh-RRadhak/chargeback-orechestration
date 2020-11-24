--------------------------------------------------------------------
--  Purpose: Internal stage for accout_usage data
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  dd/mm/yy
--------------------------------------------------------------------
use role     sysadmin;
use database gsitzman_db;
use schema   account_usage;

create or replace stage account_usage_stg;

list @account_usage_stg;
 
