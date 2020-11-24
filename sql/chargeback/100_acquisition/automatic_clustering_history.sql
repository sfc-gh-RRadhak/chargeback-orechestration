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
                        ,s.credits_used
                        ,s.num_bytes_reclustered
                        ,s.num_rows_reclustered
                        ,s.table_id
                        ,s.table_name
                        ,s.schema_id
                        ,s.schema_name
                        ,s.database_id
                        ,s.database_name
                    from
                        snowflake.account_usage.automatic_clustering_history s
                    where
                        s.end_time >= ( '{lastCtrlDt}' )
                )
                file_format      = ( type=csv field_optionally_enclosed_by = '"' )
                overwrite        = false
                single           = true