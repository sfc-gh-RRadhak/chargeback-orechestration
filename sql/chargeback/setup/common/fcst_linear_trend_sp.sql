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

create or replace procedure fcst_linear_trend_sp( P_SRC_TABLE varchar, P_TGT_TABLE varchar )
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

    try {
        // create temp table with derived forecast
        tracePos  = 'derive forecast';
        sqlCmd    = `
            create or replace temporary table identifier( '${P_TGT_TABLE}' ) as
                    -- period-to-date sequence for time
                    with l_ptd as
                    (
                        select
                             s.partition_key
                            ,s.period_dt
                            ,s.year_no
                            ,s.period_no
                            ,s.measure_no
                            ,row_number( ) over( partition by s.partition_key order by s.period_dt )    as ptd_seq_no
                        from  
                            identifier( '${P_SRC_TABLE}' ) s
                        order by
                            1,2
                    )
                    -- intercept and slope
                    ,l_int_slope as
                    (
                        select
                             partition_key
                            ,regr_intercept( measure_no, ptd_seq_no ) as intercept_no
                            ,regr_slope( measure_no, ptd_seq_no )     as slope_no
                        from  
                            l_ptd
                        where
                            measure_no is not null
                        group by 1
                        order by 1
                    )
                    --forecast
                    ,l_forecast as
                    (
                        select
                             lp.partition_key
                            ,lp.period_dt
                            ,lp.year_no
                            ,lp.period_no
                            ,lp.measure_no
                            ,lp.ptd_seq_no
                            ,lis.intercept_no
                            ,lis.slope_no
                            ,(lis.slope_no * lp.ptd_seq_no) + lis.intercept_no as fcst_measure_no
                        from
                            l_ptd lp
                            join l_int_slope lis on
                                lis.partition_key = lp.partition_key
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
                        1,2,3
            `;

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