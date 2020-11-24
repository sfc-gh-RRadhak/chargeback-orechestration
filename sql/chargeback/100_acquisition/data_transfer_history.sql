copy into
                    @{stage}/{load_name}/{fileName}
                from
                (
                    
                        select
                        'MCKESSON'             as organization_name
                        ,current_account()      as account_name
                        ,current_region()       as region_name
                        ,s.start_time
                        ,s.end_time
                        ,s.source_cloud         
                        ,s.source_region  
                        ,s.target_cloud
                        ,s.target_region        
                        ,s.bytes_transferred    
                    from
                        snowflake.account_usage.data_transfer_history s
                    where
                        s.end_time >= ( '{lastCtrlDt}' )
                )
                file_format      = ( type=csv field_optionally_enclosed_by = '"' )
                overwrite        = false
                single           = true