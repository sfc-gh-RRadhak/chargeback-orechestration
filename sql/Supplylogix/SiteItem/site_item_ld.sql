use role     {role};
use database {database};
use schema   {schema};
 
--------------------------------------------------------------------
-- insert new and update modified
--
merge into {database}.{schema}.site_item t using
(
    with l_src as
    ( 
        select
             s.dw_site_item_shk         
            ,s.dw_hash_diff              
            ,s.dw_version_ts             
            --                           
            ,s.ndc                       
            ,s.ndc_numeric
            ,s.npi        
            ,s.site_id    
            --
            ,s.site_item_json:CalculatedRefillReorderPoint::string        as calculated_refill_reorder_point              
            ,s.site_item_json:CalculatedReorderPoint::string              as calculated_reorder_point               
            ,s.site_item_json:CalculatedReorderQuantities::string         as calculated_reorder_quantities          
            ,s.site_item_json:CurrentQOH::string                          as current_q_o_h                          
            ,s.site_item_json:CurrentReorderPoint::string                 as current_reorder_point                  
            ,s.site_item_json:CurrentReorderQuantity::string              as current_reorder_quantity               
            ,s.site_item_json:DispenseCount30Days::string                 as dispense_count_30days                  
            ,s.site_item_json:DispenseCount60Days::string                 as dispense_count_60days                  
            ,s.site_item_json:DispenseCount98Days::string                 as dispense_count_98days                  
            ,s.site_item_json:DrugCode::string                            as drug_code                              
            ,s.site_item_json:HasMatchingNeed::string                     as has_matching_need                      
            ,s.site_item_json:HasMultipleRefillsInForecastWindow::string  as has_multiple_refills_in_forecast_window       
            ,s.site_item_json:IsCentralFill::string                       as is_central_fill                        
            ,s.site_item_json:IsManualOrderDetected::string               as is_manual_order_detected               
            ,s.site_item_json:IsSpecialty::string                         as is_specialty                           
            ,s.site_item_json:LastDispenseDate::string                    as last_dispense_date                     
            ,s.site_item_json:LastLinkedDispenseDate::string              as last_linked_dispense_date              
            ,s.site_item_json:LastNonPrimaryInvoiceDate::string           as last_non_primary_invoice_date          
            ,s.site_item_json:LastPrimaryAccountNumber::string            as last_primary_account_number            
            ,s.site_item_json:LastPrimaryInvoiceDate::string              as last_primary_invoice_date              
            ,s.site_item_json:LastPrimaryInvoiceNumber::string            as last_primary_invoice_number            
            ,s.site_item_json:LastPrimaryVendorItemNumber::string         as last_primary_vendor_item_number        
            ,s.site_item_json:LinkedChildNDCs::string                     as linked_child_ndcs                      
            ,s.site_item_json:LinkedParentNDC::string                     as linked_parent_ndc                      
            ,s.site_item_json:LinkedUnitsDispensed120Days::string         as linked_units_dispensed_120days         
            ,s.site_item_json:LinkedUnitsDispensed30Days::string          as linked_units_dispensed_30days          
            ,s.site_item_json:LinkedUnitsDispensed60Days::string          as linked_units_dispensed_60days          
            ,s.site_item_json:LinkedUnitsDispensed91Days::string          as linked_units_dispensed_91days          
            ,s.site_item_json:MostCommon90DaySupply::string               as most_common_90day_supply               
            ,s.site_item_json:MovementRank::string                        as movement_rank                          
            ,s.site_item_json:MultiSourceCode::string                     as multi_source_code                      
            ,s.site_item_json:OnHandValueRank::string                     as on_hand_value_rank                     
            ,s.site_item_json:Pack::string                                as pack                                   
            ,s.site_item_json:PatientCount98Days::string                  as patient_count_98days                   
            ,s.site_item_json:ProductId::string                           as product_id                             
            ,s.site_item_json:QOHLastUpdatedDate::string                  as qoh_last_updated_date                  
            ,s.site_item_json:QOHOverstockThreshold::string               as qoh_overstock_threshold                
            ,s.site_item_json:ReorderCode::string                         as reorder_code                           
            ,s.site_item_json:SalesRank::string                           as sales_rank                             
            ,s.site_item_json:Schedule::string                            as schedule                               
            ,s.site_item_json:ShouldAttenuate::string                     as should_attenuate                       
            ,s.site_item_json:UnitCost::string                            as unit_cost                              
            ,s.site_item_json:UnitsDispensed120Days::string               as units_dispensed_120days                
            ,s.site_item_json:UnitsDispensed30Days::string                as units_dispensed_30days                 
            ,s.site_item_json:UnitsDispensed60Days::string                as units_dispensed_60days                 
            ,s.site_item_json:UnitsDispensed91Days::string                as units_dispensed_91days                 
            ,s.site_item_json:VendorCode::string                          as vendor_code                            
            ,s.site_item_json:VendorInvoiceData::string                   as vendor_invoice_data                    
            ,s.site_item_json:VendorItemNumber::string                    as vendor_item_number                     
            ,s.site_item_json:VolumeRank::string                          as volume_rank                            
            --
            ,current_timestamp()                                          as dw_load_ts
            ,current_timestamp()                                          as dw_update_ts
        from
            {database}.{schema}.site_item_json s
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
                 row_number() over( partition by dw_site_item_shk order by s.dw_version_ts desc ) as seq_no
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
        left join {database}.{schema}.site_item t on
            t.dw_site_item_shk = s.dw_site_item_shk
    where
        -- source row does not exist in target table
        t.dw_site_item_shk is null
        -- or source row is more recent and differs from target table
        or (
                t.dw_version_ts  < s.dw_version_ts
            and t.dw_hash_diff  != s.dw_hash_diff
           )
) s
on
(
    t.dw_site_item_shk = s.dw_site_item_shk
)
when matched then update set
     t.dw_hash_diff                            = s.dw_hash_diff                           
    ,t.dw_version_ts                           = s.dw_version_ts                          
    --                                       
    ,t.ndc                                     = s.ndc                                    
    ,t.ndc_numeric                             = s.ndc_numeric                            
    ,t.npi                                     = s.npi                                    
    ,t.site_id                                 = s.site_id                                
    --                                       
    ,t.calculated_refill_reorder_point         = s.calculated_refill_reorder_point        
    ,t.calculated_reorder_point                = s.calculated_reorder_point               
    ,t.calculated_reorder_quantities           = s.calculated_reorder_quantities          
    ,t.current_q_o_h                           = s.current_q_o_h                          
    ,t.current_reorder_point                   = s.current_reorder_point                  
    ,t.current_reorder_quantity                = s.current_reorder_quantity               
    ,t.dispense_count_30days                   = s.dispense_count_30days                  
    ,t.dispense_count_60days                   = s.dispense_count_60days                  
    ,t.dispense_count_98days                   = s.dispense_count_98days                  
    ,t.drug_code                               = s.drug_code                              
    ,t.has_matching_need                       = s.has_matching_need                      
    ,t.has_multiple_refills_in_forecast_window = s.has_multiple_refills_in_forecast_window  
    ,t.is_central_fill                         = s.is_central_fill                        
    ,t.is_manual_order_detected                = s.is_manual_order_detected               
    ,t.is_specialty                            = s.is_specialty                           
    ,t.last_dispense_date                      = s.last_dispense_date                     
    ,t.last_linked_dispense_date               = s.last_linked_dispense_date              
    ,t.last_non_primary_invoice_date           = s.last_non_primary_invoice_date          
    ,t.last_primary_account_number             = s.last_primary_account_number            
    ,t.last_primary_invoice_date               = s.last_primary_invoice_date              
    ,t.last_primary_invoice_number             = s.last_primary_invoice_number            
    ,t.last_primary_vendor_item_number         = s.last_primary_vendor_item_number        
    ,t.linked_child_ndcs                       = s.linked_child_ndcs                      
    ,t.linked_parent_ndc                       = s.linked_parent_ndc                      
    ,t.linked_units_dispensed_120days          = s.linked_units_dispensed_120days         
    ,t.linked_units_dispensed_30days           = s.linked_units_dispensed_30days          
    ,t.linked_units_dispensed_60days           = s.linked_units_dispensed_60days          
    ,t.linked_units_dispensed_91days           = s.linked_units_dispensed_91days          
    ,t.most_common_90day_supply                = s.most_common_90day_supply               
    ,t.movement_rank                           = s.movement_rank                          
    ,t.multi_source_code                       = s.multi_source_code                      
    ,t.on_hand_value_rank                      = s.on_hand_value_rank                     
    ,t.pack                                    = s.pack                                   
    ,t.patient_count_98days                    = s.patient_count_98days                   
    ,t.product_id                              = s.product_id                             
    ,t.qoh_last_updated_date                   = s.qoh_last_updated_date                  
    ,t.qoh_overstock_threshold                 = s.qoh_overstock_threshold                
    ,t.reorder_code                            = s.reorder_code                           
    ,t.sales_rank                              = s.sales_rank                             
    ,t.schedule                                = s.schedule                               
    ,t.should_attenuate                        = s.should_attenuate                       
    ,t.unit_cost                               = s.unit_cost                              
    ,t.units_dispensed_120days                 = s.units_dispensed_120days                
    ,t.units_dispensed_30days                  = s.units_dispensed_30days                 
    ,t.units_dispensed_60days                  = s.units_dispensed_60days                 
    ,t.units_dispensed_91days                  = s.units_dispensed_91days                 
    ,t.vendor_code                             = s.vendor_code                            
    ,t.vendor_invoice_data                     = s.vendor_invoice_data                    
    ,t.vendor_item_number                      = s.vendor_item_number                     
    ,t.volume_rank                             = s.volume_rank                            
    --                                       
    ,t.dw_update_ts                            = s.dw_update_ts                           
when not matched then insert
(
     dw_site_item_shk                        
    ,dw_hash_diff                            
    ,dw_version_ts                           
    --                                       
    ,ndc                                     
    ,ndc_numeric                             
    ,npi                                     
    ,site_id                                 
    --                                       
    ,calculated_refill_reorder_point         
    ,calculated_reorder_point                
    ,calculated_reorder_quantities           
    ,current_q_o_h                           
    ,current_reorder_point                   
    ,current_reorder_quantity                
    ,dispense_count_30days                   
    ,dispense_count_60days                   
    ,dispense_count_98days                   
    ,drug_code                               
    ,has_matching_need                       
    ,has_multiple_refills_in_forecast_window 
    ,is_central_fill                         
    ,is_manual_order_detected                
    ,is_specialty                            
    ,last_dispense_date                      
    ,last_linked_dispense_date               
    ,last_non_primary_invoice_date           
    ,last_primary_account_number             
    ,last_primary_invoice_date               
    ,last_primary_invoice_number             
    ,last_primary_vendor_item_number         
    ,linked_child_ndcs                       
    ,linked_parent_ndc                       
    ,linked_units_dispensed_120days          
    ,linked_units_dispensed_30days           
    ,linked_units_dispensed_60days           
    ,linked_units_dispensed_91days           
    ,most_common_90day_supply                
    ,movement_rank                           
    ,multi_source_code                       
    ,on_hand_value_rank                      
    ,pack                                    
    ,patient_count_98days                    
    ,product_id                              
    ,qoh_last_updated_date                   
    ,qoh_overstock_threshold                 
    ,reorder_code                            
    ,sales_rank                              
    ,schedule                                
    ,should_attenuate                        
    ,unit_cost                               
    ,units_dispensed_120days                 
    ,units_dispensed_30days                  
    ,units_dispensed_60days                  
    ,units_dispensed_91days                  
    ,vendor_code                             
    ,vendor_invoice_data                     
    ,vendor_item_number                      
    ,volume_rank                             
    --
    ,dw_load_ts
    ,dw_update_ts
)
values
(
     s.dw_site_item_shk                        
    ,s.dw_hash_diff                            
    ,s.dw_version_ts                           
    --                                       
    ,s.ndc                                     
    ,s.ndc_numeric                             
    ,s.npi                                     
    ,s.site_id                                 
    --                                       
    ,s.calculated_refill_reorder_point         
    ,s.calculated_reorder_point                
    ,s.calculated_reorder_quantities           
    ,s.current_q_o_h                           
    ,s.current_reorder_point                   
    ,s.current_reorder_quantity                
    ,s.dispense_count_30days                   
    ,s.dispense_count_60days                   
    ,s.dispense_count_98days                   
    ,s.drug_code                               
    ,s.has_matching_need                       
    ,s.has_multiple_refills_in_forecast_window 
    ,s.is_central_fill                         
    ,s.is_manual_order_detected                
    ,s.is_specialty                            
    ,s.last_dispense_date                      
    ,s.last_linked_dispense_date               
    ,s.last_non_primary_invoice_date           
    ,s.last_primary_account_number             
    ,s.last_primary_invoice_date               
    ,s.last_primary_invoice_number             
    ,s.last_primary_vendor_item_number         
    ,s.linked_child_ndcs                       
    ,s.linked_parent_ndc                       
    ,s.linked_units_dispensed_120days          
    ,s.linked_units_dispensed_30days           
    ,s.linked_units_dispensed_60days           
    ,s.linked_units_dispensed_91days           
    ,s.most_common_90day_supply                
    ,s.movement_rank                           
    ,s.multi_source_code                       
    ,s.on_hand_value_rank                      
    ,s.pack                                    
    ,s.patient_count_98days                    
    ,s.product_id                              
    ,s.qoh_last_updated_date                   
    ,s.qoh_overstock_threshold                 
    ,s.reorder_code                            
    ,s.sales_rank                              
    ,s.schedule                                
    ,s.should_attenuate                        
    ,s.unit_cost                               
    ,s.units_dispensed_120days                 
    ,s.units_dispensed_30days                  
    ,s.units_dispensed_60days                  
    ,s.units_dispensed_91days                  
    ,s.vendor_code                             
    ,s.vendor_invoice_data                     
    ,s.vendor_item_number                      
    ,s.volume_rank                             
    --
    ,s.dw_load_ts
    ,s.dw_update_ts
)
;