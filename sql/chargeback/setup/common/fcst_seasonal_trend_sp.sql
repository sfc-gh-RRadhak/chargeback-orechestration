--------------------------------------------------------------------
--  Purpose: 
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  dd/mm/yy
--------------------------------------------------------------------
use role     {role};
use database {environment}_plt_common_db;
use schema   util;


create or replace procedure fcst_seasonal_trend_sp( P_SRC_TABLE varchar, P_TGT_TABLE varchar )
returns varchar
language javascript
execute as caller
as
$$
    // variables
    var status          = 'success';
    var tracePos        = 'step 0';
    var sqlResult;
    var sqlCmd;
    var mAvgLagNo;
    var mAvgLeadNo;

    try {
        // get moving avg lag and lead row counts
        // assumes model period is the same across partitions
        tracePos  = 'mavg lag rows';
        sqlCmd    = `select 
                          trunc( count( distinct period_no ) / 2 )     as mavg_lag_no 
                         ,round( count( distinct period_no ) / 2 ) - 1 as mavg_lead_no
                     from 
                         identifier( '${P_SRC_TABLE}' )`;
        sqlResult = snowflake.execute( { sqlText: sqlCmd } );
        sqlResult.next();
        mAvgLagNo   = sqlResult.getColumnValue( 'MAVG_LAG_NO' );
        mAvgLeadNo  = sqlResult.getColumnValue( 'MAVG_LEAD_NO' );
    
        // create temp table with derived forecast
        tracePos  = 'derive forecast';
        sqlCmd    = `
            create or replace temporary table identifier( '${P_TGT_TABLE}' ) as
                    -- moving average
                    with l_mavg as
                    (
                        select
                             s.partition_key
                            ,s.period_dt
                            ,s.year_no
                            ,s.period_no
                            ,s.measure_no
                            ,case
                                when lag( s.measure_no, ${mAvgLagNo} ) over( partition by s.partition_key order by s.period_dt ) is not null
                                 and lead( s.measure_no, ${mAvgLeadNo} ) over( partition by s.partition_key order by s.period_dt ) is not null
                                then avg( s.measure_no ) over( partition by s.partition_key order by s.period_dt 
                                                               rows between ${mAvgLagNo} preceding and ${mAvgLeadNo} following ) 
                                else null
                             end    as mavg_measure_no
                        from  
                            identifier( '${P_SRC_TABLE}' ) s
                        order by
                            1,2
                    )
                    -- center moving average: current row and next row
                    ,l_cmavg as
                    (
                        select 
                             partition_key
                            ,period_dt
                            ,year_no
                            ,period_no
                            ,measure_no
                            ,mavg_measure_no
                            ,case
                                when mavg_measure_no is not null
                                 and lead( mavg_measure_no, 1 ) over( partition by partition_key order by period_dt ) is not null
                                then avg( mavg_measure_no ) over( partition by partition_key order by period_dt rows between 0 preceding and 1 following ) 
                                else null
                             end    as cmavg_measure_no
                        from
                            l_mavg
                        order by
                            1,2
                    )
                    -- seasonal and irregular components
                    ,l_seasonal as
                    (
                        select 
                             partition_key
                            ,period_dt
                            ,year_no
                            ,period_no
                            ,measure_no
                            ,mavg_measure_no
                            ,cmavg_measure_no
                            ,measure_no / cmavg_measure_no  as seasonal_no
                        from
                            l_cmavg
                        order by
                            1,2
                    )
                    -- period average of seasonal
                    ,l_pavg_seasonal as
                    (
                        select 
                             partition_key
                            ,period_no
                            ,avg( seasonal_no ) as pavg_seasonal_no
                        from  
                            l_seasonal
                        where
                            seasonal_no is not null
                        group by 1,2
                        order by 1,2
                    )
                    -- deseasonalize with period avg and calc period-to-date sequence
                    ,l_deseason as
                    (
                        select
                             ls.partition_key
                            ,ls.period_dt
                            ,ls.year_no
                            ,ls.period_no
                            ,ls.measure_no
                            ,ls.mavg_measure_no
                            ,ls.cmavg_measure_no
                            ,ls.seasonal_no
                            ,lps.pavg_seasonal_no
                            ,case
                                when lps.pavg_seasonal_no is not null
                                then ls.measure_no / lps.pavg_seasonal_no                        
                                else 1
                             end                                                                                    as deseason_measure_no
                            ,row_number( ) over( partition by ls.partition_key order by ls.period_dt )  as ptd_seq_no
                        from
                            l_seasonal ls
                            left join l_pavg_seasonal lps on
                                    lps.partition_key = ls.partition_key
                                and lps.period_no     = ls.period_no
                        order by
                          1,2,3
                    )
                    -- intercept and slope
                    ,l_int_slope as
                    (
                        select
                             partition_key
                            ,regr_intercept( deseason_measure_no, ptd_seq_no ) as intercept_no
                            ,regr_slope( deseason_measure_no, ptd_seq_no )     as slope_no
                        from
                            l_deseason
                        where
                            deseason_measure_no is not null
                        group by 1
                        order by 1
                    )
                    -- trend
                    ,l_trend as
                    (
                        select
                             ld.partition_key
                            ,ld.period_dt
                            ,ld.year_no
                            ,ld.period_no
                            ,ld.measure_no
                            ,ld.mavg_measure_no
                            ,ld.cmavg_measure_no
                            ,ld.seasonal_no
                            ,ld.pavg_seasonal_no
                            ,ld.deseason_measure_no
                            ,ld.ptd_seq_no
                            ,lis.intercept_no
                            ,lis.slope_no
                            ,(lis.slope_no * ld.ptd_seq_no) + lis.intercept_no as trend_no
                        from
                            l_deseason ld
                            join l_int_slope lis on
                                lis.partition_key = ld.partition_key
                        order by
                            1,2
                    )
                    --forecast
                    ,l_forecast as
                    (
                        select
                             lt.partition_key
                            ,lt.period_dt
                            ,lt.year_no
                            ,lt.period_no
                            ,lt.measure_no
                            ,${mAvgLagNo}       as p_mavg_lag_no
                            ,${mAvgLeadNo}      as p_mavg_lead_no
                            ,lt.mavg_measure_no
                            ,lt.cmavg_measure_no
                            ,lt.seasonal_no
                            ,lt.pavg_seasonal_no
                            ,lt.deseason_measure_no
                            ,lt.ptd_seq_no
                            ,lt.intercept_no
                            ,lt.slope_no
                            ,lt.trend_no
                            ,lt.pavg_seasonal_no * lt.trend_no     as fcst_measure_no
                        from
                            l_trend lt
                        order by
                            1,2
                    )
                    select
                         lf.partition_key
                        ,lf.period_dt
                        ,lf.year_no
                        ,lf.period_no
                        ,lf.measure_no
                        ,lf.fcst_measure_no
                        ,object_construct(*)                as component_json
                    from
                        l_forecast lf
                    order by
                        1,2`;

        sqlResult = snowflake.execute( { sqlText: sqlCmd } );

    }
    catch (err) {
        status  = 'line: ' + tracePos;
        status += '\n failed: code: ' + err.code + '\n state: ' + err.state;
        status += '\n message: ' + err.message;
        status += '\n stack trace:\n' + err.stacktracetxt;
    }
    finally {
        return status;
    }                                                     
$$
;