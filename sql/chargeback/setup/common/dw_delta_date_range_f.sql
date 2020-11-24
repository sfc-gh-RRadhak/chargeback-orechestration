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


create or replace function dw_delta_date_range_f
( 
    p_period_type_cd   varchar 
)
returns table( start_dt date, end_dt date )
as
$$
    select
         start_dt
        ,end_dt
    from
        (
        select
             case lower( p_period_type_cd )
                 when 'all'     then current_date()
                 when 'day'     then date_trunc( day, event_dt )
                 when 'week'    then date_trunc( week, event_dt )
                 when 'month'   then date_trunc( month, event_dt )
                 when 'quarter' then date_trunc( quarter, event_dt )
                 when 'year'    then date_trunc( year, event_dt )
                 else current_date()
             end                as partition_dt
            ,min( event_dt ) as start_dt
            ,max( event_dt ) as end_dt
        from
            {environment}_plt_common_db.util.dw_delta_date
        group by
            1
        )
    order by
        1
$$
;
