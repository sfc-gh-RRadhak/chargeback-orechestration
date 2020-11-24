truncate table {environment}_plt_rl_snowflake_db.account_usage.login_history_stg;
copy into
                {environment}_plt_rl_snowflake_db.account_usage.login_history_stg
            from
                (
                select
                    $1                                      -- organization_name 
                    ,$2                                     -- account_name  
                    ,$3                                     -- region_name  
                    ,$4                                     -- event_timestamp  
                    ,$5                                     -- event_id  
                    ,$6                                     -- event_type 
                    ,$7                                     -- user_name
                    ,$8                                     -- client_ip  
                    ,$9                                     -- reported_client_type  
                    ,$10                                     -- reported_client_version  
                    ,$11                                    -- first_authentication_factor 
                    ,$12                                    -- second_authentication_factor  
                    ,$13                                    -- is_success  
                    ,$14                                    -- error_code  
                    ,$15                                    -- error_message  
                    ,$16                                    -- related_event_id  
                    ,metadata$filename                      -- dw_file_name
                    ,metadata$file_row_number               -- dw_file_row_no
                    ,current_timestamp()                    -- dw_load_ts
                from
                    @{stage}/login_history s
                )
            file_format = ( type=csv field_optionally_enclosed_by = '"' )
            purge = true;