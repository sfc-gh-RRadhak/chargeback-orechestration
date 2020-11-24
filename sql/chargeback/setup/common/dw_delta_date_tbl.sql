--------------------------------------------------------------------
--  Purpose: create psa tables
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  dd/mm/yy
--------------------------------------------------------------------

use role     {role};
use database {environment}_plt_common_db;
use schema   util;

create or replace transient table dw_delta_date
(
     event_dt       date          not null
    ,dw_load_ts     timestamp_ltz not null
)
data_retention_time_in_days = 0
copy grants
;