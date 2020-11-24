truncate table {environment}_plt_rl_snowflake_db.account_usage.query_history_stg;
copy into
                {environment}_plt_rl_snowflake_db.account_usage.query_history_stg
            from
                (
                select
                        $1 --organization_name
                        ,$2 --account_name
                        ,$3 --region_name
                        ,$4 --query_id
                        ,$5 --query_text
                        ,$6 --database_id
                        ,$7 --database_name
                        ,$8 --schema_id
                        ,$9 --schema_name
                        ,$10 --query_type
                        ,$11 --session_id
                        ,$12 --user_name
                        ,$13 --role_name
                        ,$14 --warehouse_id
                        ,$15 --warehouse_name
                        ,$16 --warehouse_size
                        ,$17 --warehouse_type
                        ,$18 --cluster_number
                        ,$19 --query_tag
                        ,$20 --execution_status
                        ,$21 --error_code
                        ,$22 --error_message
                        ,$23 --start_time
                        ,$24 --end_time
                        ,$25 --total_elapsed_time
                        ,$26 --bytes_scanned
                        ,$27 --percentage_scanned_from_cache
                        ,$28 --bytes_written
                        ,$29 --bytes_written_to_result
                        ,$30 --bytes_read_from_result
                        ,$31 --rows_produced
                        ,$32 --rows_inserted
                        ,$33 --rows_updated
                        ,$34 --rows_deleted
                        ,$35 --rows_unloaded
                        ,$36 --bytes_deleted
                        ,$37 --partitions_scanned
                        ,$38 --partitions_total
                        ,$39 --bytes_spilled_to_local_storage
                        ,$40 --bytes_spilled_to_remote_storage
                        ,$41 --bytes_sent_over_the_network
                        ,$42 --compilation_time
                        ,$43 --execution_time
                        ,$44 --queued_provisioning_time
                        ,$45 --queued_repair_time
                        ,$46 --queued_overload_time
                        ,$47 --transaction_blocked_time
                        ,$48 --outbound_data_transfer_cloud
                        ,$49 --outbound_data_transfer_region
                        ,$50 --outbound_data_transfer_bytes
                        ,$51 --inbound_data_transfer_cloud
                        ,$52 --inbound_data_transfer_region
                        ,$53 --inbound_data_transfer_bytes
                        ,$54 --list_external_files_time
                        ,$55 --credits_used_cloud_services
                        ,$56 --release_version
                        ,$57 --external_function_total_invocations
                        ,$58 --external_function_total_sent_rows
                        ,$59 --external_function_total_received_rows
                        ,$60 --external_function_total_sent_bytes
                        ,$61 --external_function_total_received_bytes
                        ,$62 --query_load_percent
                        ,$63 --is_client_generated_statement
                    ,metadata$filename                      -- dw_file_name
                    ,metadata$file_row_number               -- dw_file_row_no
                    ,current_timestamp()                    -- dw_load_ts
                from
                    @{stage}/query_history s
                )
            file_format = ( type=csv field_optionally_enclosed_by = '"' )
            purge = true;