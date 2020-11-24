copy into
                    @{stage}/{load_name}/{fileName}
                from
                (
                    select
                        'MCKESSON'             as organization_name
                        ,current_account()      as account_name
                        ,current_region()       as region_name
                        ,s.usage_date
                        ,s.database_id
                        ,s.database_name
                        ,s.deleted
                        ,s.average_database_bytes
                        ,s.average_failsafe_bytes
                    from
                        snowflake.account_usage.database_storage_usage_history s
                    where
                        s.usage_date >=  '{lastCtrlDt}' 
                )
                file_format      = ( type=csv field_optionally_enclosed_by = '"' )
                overwrite        = false
                single           = true