USE role     {role};
USE database {database};
USE schema   {schema};

--------------------------------------------------------------------
-- insert new and update modified
--
merge into {database}.{schema}.site_item_dispense t using
(
    with l_src as
    ( 
        select
             sha1_binary( concat( to_char( s.dw_site_item_shk )
                                 ,'|'
                                 ,sd.key::string
                                 ,'|'
                                 ,si.value:SecondaryId::string ) 
                        )                                                 as dw_site_item_dispense_shk
            ,sha1_binary( concat( to_char( s.dw_site_item_shk )
                                 ,'|'
                                 ,sd.key::string
                                 ,'|'
                                 ,si.value:SecondaryId::string
                                 ,'|'
                                 ,si.value ) 
                        )                                                 as dw_hash_diff
            ,s.dw_site_item_shk         
            ,s.dw_version_ts             
            --                           
            ,s.ndc                       
            --
            ,sd.key::string                                               as dispense_dt
            ,si.value:DaysSupplyCategory::string                          as display_supply_category
            ,si.value:DispenseQuantity::string                            as dispense_quantity
            ,si.value:DispenseTime::string                                as dispense_time
            ,si.value:EncodedPrescriberDEA::string                        as encoded_prescriber_dea
            ,si.value:PatientHashCode::string                             as patient_hash_code
            ,si.value:PrescriberNPI::string                               as prescriber_npi
            ,si.value:SecondaryId::string                                 as secondary_id
            --
            ,si.index                                                     as dw_json_index_no
            ,current_timestamp()                                          as dw_load_ts
            ,current_timestamp()                                          as dw_update_ts
        from
             {database}.{schema}.site_item_json s
            ,lateral flatten( input => s.dispense_detail_json:Dispenses ) sd
            ,lateral flatten( input => sd.value ) si 
        where
            -- pull recent changes
            s.dw_version_ts >= dateadd( day, -7, current_timestamp() )
    )
    ,l_latest as
    (
        select
            *
        from
            (
            select
                 -- identify dupes and only keep copy 1
                 row_number() over( partition by dw_site_item_dispense_shk order by s.dw_version_ts desc ) as seq_no
                ,s.*
            from
                l_src s
            )
        where
            seq_no = 1 -- keep only unique rows
    )
    select
        s.*
    from
        l_latest s
        left join {database}.{schema}.site_item_dispense t on
            t.dw_site_item_dispense_shk = s.dw_site_item_dispense_shk
    where
        -- source row does not exist in target table
        t.dw_site_item_dispense_shk is null
        -- or source row is more recent and differs from target table
        or (
                t.dw_version_ts  < s.dw_version_ts and t.dw_hash_diff  != s.dw_hash_diff
           )
) s
on
(
    t.dw_site_item_dispense_shk = s.dw_site_item_dispense_shk
)
when matched then update set
     t.dw_site_item_shk                    = s.dw_site_item_shk                    
    ,t.dw_hash_diff                        = s.dw_hash_diff                        
    ,t.dw_version_ts                       = s.dw_version_ts                       
    --                                    
    ,t.ndc                                 = s.ndc                                 
    --                                     
    ,t.dispense_dt                         = s.dispense_dt            
    ,t.display_supply_category             = s.display_supply_category
    ,t.dispense_quantity                   = s.dispense_quantity      
    ,t.dispense_time                       = s.dispense_time          
    ,t.encoded_prescriber_dea              = s.encoded_prescriber_dea 
    ,t.patient_hash_code                   = s.patient_hash_code      
    ,t.prescriber_npi                      = s.prescriber_npi         
    ,t.secondary_id                        = s.secondary_id           
    --    
    ,t.dw_json_index_no                    = s.dw_json_index_no                                
    ,t.dw_update_ts                        = s.dw_update_ts                        
when not matched then insert
(
     dw_site_item_dispense_shk
    ,dw_site_item_shk                   
    ,dw_hash_diff                       
    ,dw_version_ts                      
    --                                  
    ,ndc                                
    --                                  
    ,dispense_dt
    ,display_supply_category
    ,dispense_quantity
    ,dispense_time
    ,encoded_prescriber_dea
    ,patient_hash_code
    ,prescriber_npi
    ,secondary_id
    --
    ,dw_json_index_no
    ,dw_load_ts                         
    ,dw_update_ts                       
)
values
(
     s.dw_site_item_dispense_shk
    ,s.dw_site_item_shk                   
    ,s.dw_hash_diff                       
    ,s.dw_version_ts                      
    --                                  
    ,s.ndc                                
    --                                  
    ,s.dispense_dt
    ,s.display_supply_category
    ,s.dispense_quantity
    ,s.dispense_time
    ,s.encoded_prescriber_dea
    ,s.patient_hash_code
    ,s.prescriber_npi
    ,s.secondary_id
    --
    ,s.dw_json_index_no
    ,s.dw_load_ts                         
    ,s.dw_update_ts                       
)
;
