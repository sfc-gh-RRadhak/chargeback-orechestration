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

create or replace function date_sid_f
( 
    p_date             date 
)
returns number
as
$$
    select to_number( to_char( p_date, 'yyyymmdd') )
$$
;

select
     current_date()                         as todays_dt
    ,date_sid_f( current_date() )           as dw_date_sid
    --
    ,date_trunc( month, current_date() )    as todays_month_dt
    ,date_sid_f( date_trunc( month, current_date() ) )     as dw_month_date_sid
;
