insert into {environment}_plt_rl_snowflake_db.account_usage.query_history
            with l_stg as
            (
                select 
                     sha1_binary( concat( s.account_name
                                         ,s.organization_name
                                         ,'|', to_char( s.query_id ) 
                                        )
                                )               as dw_event_shk
                    ,s.*
                from
                    {environment}_plt_rl_snowflake_db.account_usage.query_history_stg s
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
                , s.organization_name
                , s.account_name
                , s.region_name
                , s.query_id
                , s.query_text
                , s.database_id
                , s.database_name
                , s.schema_id
                , s.schema_name
                , s.query_type
                , s.session_id
                , s.user_name
                , s.role_name
                , s.warehouse_id
                , s.warehouse_name
                , s.warehouse_size
                , s.warehouse_type
                , s.cluster_number
                , s.query_tag
                , s.execution_status
                , s.error_code
                , s.error_message
                , s.start_time
                , s.end_time
                , s.total_elapsed_time
                , s.bytes_scanned
                , s.percentage_scanned_from_cache
                , s.bytes_written
                , s.bytes_written_to_result
                , s.bytes_read_from_result
                , s.rows_produced
                , s.rows_inserted
                , s.rows_updated
                , s.rows_deleted
                , s.rows_unloaded
                , s.bytes_deleted
                , s.partitions_scanned
                , s.partitions_total
                , s.bytes_spilled_to_local_storage
                , s.bytes_spilled_to_remote_storage
                , s.bytes_sent_over_the_network
                , s.compilation_time
                , s.execution_time
                , s.queued_provisioning_time
                , s.queued_repair_time
                , s.queued_overload_time
                , s.transaction_blocked_time
                , s.outbound_data_transfer_cloud
                , s.outbound_data_transfer_region
                , s.outbound_data_transfer_bytes
                , s.inbound_data_transfer_cloud
                , s.inbound_data_transfer_region
                , s.inbound_data_transfer_bytes
                , s.list_external_files_time
                , s.credits_used_cloud_services
                , s.release_version
                , s.external_function_total_invocations
                , s.external_function_total_sent_rows
                , s.external_function_total_received_rows
                , s.external_function_total_sent_bytes
                , s.external_function_total_received_bytes
                , s.query_load_percent
                , s.is_client_generated_statement

                , s.dw_file_name                    
                , s.dw_file_row_no                                    
                , current_timestamp()    as dw_load_ts
            from
                l_deduped s
            where
                s.dw_event_shk not in
                (
                    select dw_event_shk from {environment}_plt_rl_snowflake_db.account_usage.query_history
                )
            order by
                end_time;