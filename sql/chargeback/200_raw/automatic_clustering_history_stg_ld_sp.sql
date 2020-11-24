truncate table {environment}_plt_rl_snowflake_db.account_usage.automatic_clustering_history_stg;
copy into
                {environment}_plt_rl_snowflake_db.account_usage.automatic_clustering_history_stg
            from
                (
                select
                     s.$1                                            -- organization_name      
                    ,s.$2                                            -- account_name                
                    ,s.$3                                            -- region_name          
                    ,s.$4                                            -- start_time            
                    ,s.$5                                            -- end_time              
                    ,s.$6                                            -- credits_used          
                    ,s.$7                                            -- num_bytes_reclustered 
                    ,s.$8                                            -- num_rows_reclustered  
                    ,s.$9                                            -- table_id              
                    ,s.$10                                           -- table_name           
                    ,s.$11                                           -- schema_id            
                    ,s.$12                                           -- schema_name          
                    ,s.$13                                           -- database_id          
                    ,s.$14                                           -- database_name           
                    ,metadata$filename                               -- dw_file_name
                    ,metadata$file_row_number                        -- dw_file_row_no
                    ,current_timestamp()                             -- dw_load_ts
                from
                    @{stage}/automatic_clustering_history s
                )
            file_format = ( type=csv field_optionally_enclosed_by = '"' )
            purge = true;