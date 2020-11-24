
set (l_start_dt, l_end_dt ) = (select start_dt, end_dt + 1 from table( {environment}_plt_common_db.util.dw_delta_date_range_f( 'all' ) ));

insert into 
    {environment}_plt_rl_snowflake_db.account_usage.data_transfer_history
with l_stg as
(
    select
        -- generate hash key and hash diff to streamline processing
         sha1_binary( concat( s.account_name
                             ,'|', s.organization_name
                             ,'|', ifnull( s.target_cloud, '?' )
                             ,'|', ifnull( s.target_region, '?' )
                             ,'|', to_char( s.end_time, 'yyyy-mmm-dd hh24:mi:ss.FF3 TZHTZM'  )
                            )
                    )               as dw_event_shk
        ,s.*
    from
        {environment}_plt_rl_snowflake_db.account_usage.data_transfer_history_stg s
)
,l_deduped as
(
    select
        *
    from
        (
        select
             -- identify dupes and only keep copy 1
             row_number() over( partition by dw_event_shk order by 1 ) as seq_no
            ,s.*
        from
            l_stg s
        )
    where
        seq_no = 1 -- keep only unique rows
)
select
     s.dw_event_shk
    ,s.organization_name
    ,s.account_name               
    ,s.region_name                
    ,s.start_time                 
    ,s.end_time                   
    ,s.source_cloud     
    ,s.source_region    
    ,s.target_cloud     
    ,s.target_region    
    ,s.bytes_transferred
    ,s.dw_file_name
    ,s.dw_file_row_no
    ,current_timestamp()    as dw_load_ts
from
    l_deduped s
where
    s.dw_event_shk not in
    (
        select dw_event_shk from {environment}_plt_rl_snowflake_db.account_usage.data_transfer_history where end_time >= $l_start_dt and end_time < $l_end_dt
    )
order by
    end_time  -- physically sort rows by a logical partitioning date
;

 