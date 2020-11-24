copy into @{stage}/{load_name}/{fileName}
                from
                ( select  'MCKESSON'             as organization_name
                        ,current_account()      as account_name
                        ,current_region()       as region_name
                        ,s.start_time
                        ,s.end_time
                        ,s.warehouse_id
                        ,s.warehouse_name
                        ,s.credits_used
                        ,s.credits_used_compute
                        ,s.credits_used_cloud_services
                    from
                        snowflake.account_usage.warehouse_metering_history s
                    where
                        s.end_time >=  '{lastCtrlDt}' 
                )
                file_format      = ( type=csv field_optionally_enclosed_by = '"' )
                overwrite        = false
                single           = true