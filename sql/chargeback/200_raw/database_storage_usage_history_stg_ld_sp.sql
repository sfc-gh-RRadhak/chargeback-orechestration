truncate table {environment}_plt_rl_snowflake_db.account_usage.database_storage_usage_history_stg;
copy into
                {environment}_plt_rl_snowflake_db.account_usage.database_storage_usage_history_stg
            from
                (
                select
                     s.$1                                            -- organization_name     
                    ,s.$2                                            -- account_name                
                    ,s.$3                                            -- region_name                 
                    ,s.$4                                            -- usage_date                   
                    ,s.$5                                            -- database_id                  
                    ,s.$6                                            -- database_name                
                    ,s.$7                                            -- deleted                      
                    ,s.$8                                            -- average_database_bytes   
                    ,s.$9                                            -- average_failsafe_bytes    
                    ,metadata$filename                               -- dw_file_name
                    ,metadata$file_row_number                        -- dw_file_row_no
                    ,current_timestamp()                             -- dw_load_ts
                from
                    @{stage}/database_storage_usage_history s
                )
            file_format = ( type=csv field_optionally_enclosed_by = '"' )
            purge = true