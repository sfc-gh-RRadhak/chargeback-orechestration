truncate table {environment}_plt_rl_snowflake_db.account_usage.data_transfer_history_stg;
copy into
                {environment}_plt_rl_snowflake_db.account_usage.data_transfer_history_stg
            from
                (
                select
                     s.$1                                            -- organization_name 
                    ,s.$2                                            -- account_name                
                    ,s.$3                                            -- region_name               
                    ,s.$4                                            -- start_time                 
                    ,s.$5                                            -- end_time                   
                    ,s.$6                                            -- source_cloud           
                    ,s.$7                                            -- source_region 
                    ,s.$8                                            -- target_cloud        
                    ,s.$9                                            -- target_region          
                    ,s.$10                                           -- bytes_transferred      
                    ,metadata$filename                               -- dw_file_name
                    ,metadata$file_row_number                        -- dw_file_row_no
                    ,current_timestamp()                             -- dw_load_ts
                from
                    @{stage}/data_transfer_history s
                )
            file_format = ( type=csv field_optionally_enclosed_by = '"' )
            purge = true ;