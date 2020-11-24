  
set (l_start_dt, l_end_dt ) = (select start_dt, end_dt + 1 from table( {environment}_plt_common_db.util.dw_delta_date_range_f( 'all' ) ));

 
insert into 
    {environment}_plt_rl_snowflake_db.account_usage.database_storage_usage_history
with l_stg as
(
    select
        -- generate hash key and hash diff to streamline processing
         sha1_binary( concat( s.account_name
                             ,'|', s.organization_name
                             ,'|', to_char( s.database_id )
                             ,'|', to_char( s.usage_date, 'yyyy-mmm-dd'  )
                            )
                    )               as dw_event_shk
        ,s.*
    from
        {environment}_plt_rl_snowflake_db.account_usage.database_storage_usage_history_stg s
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
    ,s.usage_date                   
    ,s.database_id                  
    ,s.database_name                
    ,s.deleted                      
    ,s.average_database_bytes       
    ,s.average_failsafe_bytes       
    ,s.dw_file_name
    ,s.dw_file_row_no
    ,current_timestamp()    as dw_load_ts
from
    l_deduped s
where
    s.dw_event_shk not in
    (
        select dw_event_shk from {environment}_plt_rl_snowflake_db.account_usage.database_storage_usage_history where usage_date >= $l_start_dt and usage_date < $l_end_dt
    )
order by
    usage_date  -- physically sort rows by a logical partitioning date
;

