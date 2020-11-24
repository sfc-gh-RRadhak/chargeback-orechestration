merge into {environment}_plt_pl_db.main.cb_service_type_dm t using
(
    with l_stg as
    (
        select
             sha1_binary( concat( s.service_type_cd
                                 ,'|', s.service_type_name
                                 ,'|', s.service_type_group_name
                                 ,'|', to_char( s.active_dt, 'yyyymmdd' )
                                 ,'|', to_char( s.inactive_dt, 'yyyymmdd' )
                                )
                        )                       as dw_hash_diff
            --
            ,s.dw_service_type_shk
            ,s.service_type_cd
            ,s.service_type_name
            ,s.service_type_group_name
            ,s.active_dt
            ,s.inactive_dt
            ,s.dw_load_ts
            ,s.dw_update_ts              
        from
            {environment}_plt_rl_snowflake_db.account_usage.cb_service_type_lkp s
    )
    ,l_deduped as
    (
        select
            *
        from
            (
            select
                 -- identify dupes and only keep copy 1
                 -- note this is deduping on the primary key
                 row_number() over( partition by s.dw_service_type_shk order by s.active_dt desc ) as seq_no
                ,s.*
            from
                l_stg s
            )
        where
            seq_no = 1 -- keep only unique rows
    )
    select
         current_timestamp()        as dw_version_ts
        ,s.*
    from
        l_deduped s
        left join {environment}_plt_pl_db.main.cb_service_type_dm t on
            t.dw_service_type_shk = s.dw_service_type_shk
    where
        -- source row does not exist in target table
        t.dw_service_type_shk is null
        -- or source row is more recent and differs from target table
        or (
                t.dw_update_ts      < s.dw_update_ts
            and t.dw_hash_diff     != s.dw_hash_diff
           )
    order by
        s.active_dt
) s
on
(
    t.dw_service_type_shk = s.dw_service_type_shk
)
when matched then update set
     t.dw_hash_diff             = s.dw_hash_diff
    ,t.service_type_cd          = s.service_type_cd        
    ,t.service_type_name        = s.service_type_name      
    ,t.service_type_group_name  = s.service_type_group_name
    ,t.active_dt                = s.active_dt              
    ,t.inactive_dt              = s.inactive_dt
    --
    ,t.dw_update_ts             = current_timestamp()
when not matched then insert
(
     dw_service_type_shk
    ,dw_hash_diff
    ,service_type_cd        
    ,service_type_name      
    ,service_type_group_name
    ,active_dt              
    ,inactive_dt            
    ,dw_load_ts
    ,dw_update_ts
)
values
(
     s.dw_service_type_shk
    ,s.dw_hash_diff
    ,s.service_type_cd        
    ,s.service_type_name      
    ,s.service_type_group_name
    ,s.active_dt              
    ,s.inactive_dt            
    ,current_timestamp()
    ,current_timestamp()
)
;