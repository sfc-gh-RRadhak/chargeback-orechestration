--------------------------------------------------------------------
--  Purpose: load master_asn
--
--  Revision History:
--  Date     Engineer      Description
--  -------- ------------- ----------------------------------
--  dd/mm/yy
--------------------------------------------------------------------
use role     {role};
use database {database};
use schema   {schema};
 
--------------------------------------------------------------------
-- insert new and update modified
--
merge into {database}.{schema}.chain_item t using
(
    with l_src as
    ( 
        select
             s.dw_chain_item_shk         
            ,s.dw_hash_diff              
            ,s.dw_version_ts             
            --                           
            ,s.ndc                       
            ,s.is_transferable           
            ,s.pack_size                 
            ,s.suggested_reorder_quantity
            ,s.unit_cost                 
            ,s.item_no                   
            --
            ,s.item_json:CalculatedRefillReorderPoint::string        as calculated_refill_reorder_point         
            ,s.item_json:CalculatedReorderPoint::string              as calculated_reorder_point                
            ,s.item_json:CalculatedReorderQuantities::string         as calculated_reorder_quantities           
            ,s.item_json:CurrentQOH::string                          as current_qoh                             
            ,s.item_json:CurrentReorderPoint::string                 as current_reorder_point                   
            ,s.item_json:CurrentReorderQuantity::string              as current_reorder_quantity                
            ,s.item_json:HasMatchingNeed::string                     as has_matching_need                       
            ,s.item_json:HasMultipleRefillsInForecastWindow::string  as has_multiple_refills_in_forecast_window 
            ,s.item_json:IsAggressiveForecastingAllowed::string      as is_aggressive_forecasting_allowed       
            ,s.item_json:IsCentralFill::string                       as is_central_fill                         
            ,s.item_json:IsManualOrderDetected::string               as is_manual_order_detected                
            ,s.item_json:IsPrimaryVendorCode::string                 as is_primary_vendor_code                  
            ,s.item_json:IsSpecialty::string                         as is_specialty                            
            ,s.item_json:LastLinkedDispenseDate::string              as last_linked_dispense_date               
            ,s.item_json:LastNonPrimaryInvoiceDate::string           as last_nonprimary_invoice_date            
            ,s.item_json:LastPrimaryInvoiceDate::string              as last_primary_invoice_date               
            ,s.item_json:Linking::string                             as linking                                 
            ,s.item_json:QOHOverstockThreshold::string               as qoh_overstock_threshold                 
            ,s.item_json:ShouldAttenuate::string                     as should_attenuate                        
            ,s.item_json:TriggerDispenses::string                    as trigger_dispenses                       
            ,s.item_json:UnitsDispensed120Days::string               as units_dispensed_120days                 
            ,s.item_json:UnitsDispensed30Days::string                as units_dispensed_30days                  
            ,s.item_json:UnitsDispensed60Days::string                as units_dispensed_60days                  
            ,s.item_json:UnitsDispensed91Days::string                as units_dispensed_91days                  
            ,s.item_json:VendorCode::string                          as vendor_code                             
            ,s.item_json:VendorInvoiceData::string                   as vendor_invoice_data                                 --
            --
            ,current_timestamp()                                     as dw_load_ts
            ,current_timestamp()                                     as dw_update_ts
        from
            {database}.{schema}.chain_item_json s
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
                 row_number() over( partition by dw_chain_item_shk order by s.dw_version_ts desc ) as seq_no
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
        left join {database}.{schema}.chain_item t on
            t.dw_chain_item_shk = s.dw_chain_item_shk
    where
        -- source row does not exist in target table
        t.dw_chain_item_shk is null
        -- or source row is more recent and differs from target table
        or (
                t.dw_version_ts  < s.dw_version_ts
            and t.dw_hash_diff  != s.dw_hash_diff
           )
    order by
         s.ndc
        ,s.item_no
) s
on
(
    t.dw_chain_item_shk = s.dw_chain_item_shk
)
when matched then update set
     t.dw_hash_diff                            = s.dw_hash_diff                           
    ,t.dw_version_ts                           = s.dw_version_ts                          
    --                                       
    ,t.ndc                                     = s.ndc                                    
    ,t.is_transferable                         = s.is_transferable                        
    ,t.pack_size                               = s.pack_size                              
    ,t.suggested_reorder_quantity              = s.suggested_reorder_quantity             
    ,t.unit_cost                               = s.unit_cost                              
    ,t.item_no                                 = s.item_no                                
    --                                       
    ,t.calculated_refill_reorder_point         = s.calculated_refill_reorder_point        
    ,t.calculated_reorder_point                = s.calculated_reorder_point               
    ,t.calculated_reorder_quantities           = s.calculated_reorder_quantities          
    ,t.current_qoh                             = s.current_qoh                            
    ,t.current_reorder_point                   = s.current_reorder_point                  
    ,t.current_reorder_quantity                = s.current_reorder_quantity               
    ,t.has_matching_need                       = s.has_matching_need                      
    ,t.has_multiple_refills_in_forecast_window = s.has_multiple_refills_in_forecast_window
    ,t.is_aggressive_forecasting_allowed       = s.is_aggressive_forecasting_allowed      
    ,t.is_central_fill                         = s.is_central_fill                        
    ,t.is_manual_order_detected                = s.is_manual_order_detected               
    ,t.is_primary_vendor_code                  = s.is_primary_vendor_code                 
    ,t.is_specialty                            = s.is_specialty                           
    ,t.last_linked_dispense_date               = s.last_linked_dispense_date              
    ,t.last_nonprimary_invoice_date            = s.last_nonprimary_invoice_date           
    ,t.last_primary_invoice_date               = s.last_primary_invoice_date              
    ,t.linking                                 = s.linking                                
    ,t.qoh_overstock_threshold                 = s.qoh_overstock_threshold                
    ,t.should_attenuate                        = s.should_attenuate                       
    ,t.trigger_dispenses                       = s.trigger_dispenses                      
    ,t.units_dispensed_120days                 = s.units_dispensed_120days                
    ,t.units_dispensed_30days                  = s.units_dispensed_30days                 
    ,t.units_dispensed_60days                  = s.units_dispensed_60days                 
    ,t.units_dispensed_91days                  = s.units_dispensed_91days                 
    ,t.vendor_code                             = s.vendor_code                            
    ,t.vendor_invoice_data                     = s.vendor_invoice_data                    
    --                                       
    ,t.dw_update_ts                            = s.dw_update_ts                           
when not matched then insert
(
     dw_chain_item_shk                       
    ,dw_hash_diff                            
    ,dw_version_ts                           
    --                                       
    ,ndc                                     
    ,is_transferable                         
    ,pack_size                               
    ,suggested_reorder_quantity              
    ,unit_cost                               
    ,item_no                                 
    --                                       
    ,calculated_refill_reorder_point         
    ,calculated_reorder_point                
    ,calculated_reorder_quantities           
    ,current_qoh                             
    ,current_reorder_point                   
    ,current_reorder_quantity                
    ,has_matching_need                       
    ,has_multiple_refills_in_forecast_window 
    ,is_aggressive_forecasting_allowed       
    ,is_central_fill                         
    ,is_manual_order_detected                
    ,is_primary_vendor_code                  
    ,is_specialty                            
    ,last_linked_dispense_date               
    ,last_nonprimary_invoice_date            
    ,last_primary_invoice_date               
    ,linking                                 
    ,qoh_overstock_threshold                 
    ,should_attenuate                        
    ,trigger_dispenses                       
    ,units_dispensed_120days                 
    ,units_dispensed_30days                  
    ,units_dispensed_60days                  
    ,units_dispensed_91days                  
    ,vendor_code                             
    ,vendor_invoice_data                     
    --
    ,dw_load_ts                              
    ,dw_update_ts                            
)
values
(
     s.dw_chain_item_shk                       
    ,s.dw_hash_diff                            
    ,s.dw_version_ts                           
    --                                       
    ,s.ndc                                     
    ,s.is_transferable                         
    ,s.pack_size                               
    ,s.suggested_reorder_quantity              
    ,s.unit_cost                               
    ,s.item_no                                 
    --                                       
    ,s.calculated_refill_reorder_point         
    ,s.calculated_reorder_point                
    ,s.calculated_reorder_quantities           
    ,s.current_qoh                             
    ,s.current_reorder_point                   
    ,s.current_reorder_quantity                
    ,s.has_matching_need                       
    ,s.has_multiple_refills_in_forecast_window 
    ,s.is_aggressive_forecasting_allowed       
    ,s.is_central_fill                         
    ,s.is_manual_order_detected                
    ,s.is_primary_vendor_code                  
    ,s.is_specialty                            
    ,s.last_linked_dispense_date               
    ,s.last_nonprimary_invoice_date            
    ,s.last_primary_invoice_date               
    ,s.linking                                 
    ,s.qoh_overstock_threshold                 
    ,s.should_attenuate                        
    ,s.trigger_dispenses                       
    ,s.units_dispensed_120days                 
    ,s.units_dispensed_30days                  
    ,s.units_dispensed_60days                  
    ,s.units_dispensed_91days                  
    ,s.vendor_code                             
    ,s.vendor_invoice_data                     
    --
    ,s.dw_load_ts                              
    ,s.dw_update_ts                            
)
;
