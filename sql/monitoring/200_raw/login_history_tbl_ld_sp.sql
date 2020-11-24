insert into {environment}_plt_rl_snowflake_db.account_usage.login_history
            with l_stg as
            (
                select 
                     sha1_binary( concat( s.account_name
                                         ,s.organization_name
                                         ,'|', to_char( s.event_id ) 
                                        )
                                )               as dw_event_shk
                    ,s.*
                from
                    {environment}_plt_rl_snowflake_db.account_usage.login_history_stg s
            )
            ,l_deduped as
            (
                select
                    *
                from
                    (
                    select
                          
                         row_number() over( partition by dw_event_shk order by 1 ) as seq_no
                        ,s.*
                    from
                        l_stg s
                    )
                where
                    seq_no = 1  
            )
            select
                 s.dw_event_shk
                ,s.organization_name  
                ,s.account_name  
                ,s.region_name  
                ,s.event_timestamp  
                ,s.event_id  
                ,s.event_type 
                ,s.user_name  
                ,s.client_ip  
                ,s.reported_client_type  
                ,s.reported_client_version  
                ,s.first_authentication_factor  
                ,s.second_authentication_factor  
                ,s.is_success  
                ,s.error_code  
                ,s.error_message  
                ,s.related_event_id  
                --
                ,s.dw_file_name                    
                ,s.dw_file_row_no                                     
                ,current_timestamp()    as dw_load_ts
            from
                l_deduped s
            where
                s.dw_event_shk not in
                (
                    select dw_event_shk from {environment}_plt_rl_snowflake_db.account_usage.login_history
                )
            order by
                event_timestamp;