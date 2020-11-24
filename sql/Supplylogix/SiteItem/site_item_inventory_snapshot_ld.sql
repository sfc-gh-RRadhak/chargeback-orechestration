USE role     {role};
USE database {database};
USE schema   {schema};
 
--------------------------------------------------------------------
-- insert new and update modified
--
merge into {database}.{schema}.site_item_inventory_snapshot t using
(
    with l_src as
    ( 
        select
             sha1_binary( concat( to_char( s.dw_site_item_shk )
                                 ,'|'
                                 ,si.value:Date::string ) 
                        )    as dw_site_item_inventory_snapshot_shk
            ,sha1_binary( concat( to_char( s.dw_site_item_shk )
                                 ,'|'
                                 ,si.value:Date::string
                                 ,'|'
                                 ,si.value ) 
                        )       as dw_hash_diff
            ,s.dw_site_item_shk         
            ,s.dw_version_ts             
            --                           
            ,s.ndc                       
            --
            ,si.value:Date::string              as qoh_date    
            ,si.value:Qoh::string               as qoh             
            ,si.value:ReorderPoint::string      as reorder_point            
            ,si.value:ReorderQuantity::string   as reorder_quantity            
            ,si.value:UnitCost::string          as unit_cost            
            --
            ,si.index                                                     as dw_json_index_no
            ,current_timestamp()                                          as dw_load_ts
            ,current_timestamp()                                          as dw_update_ts
        from
             {database}.{schema}.site_item_json s
            ,lateral flatten( input => s.inventory_detail_json, outer => true ) si
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
                 row_number() over( partition by dw_site_item_inventory_snapshot_shk order by s.dw_version_ts desc ) as seq_no
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
        left join {database}.{schema}.site_item_inventory_snapshot t on
            t.dw_site_item_inventory_snapshot_shk = s.dw_site_item_inventory_snapshot_shk
    where
        -- source row does not exist in target table
        t.dw_site_item_inventory_snapshot_shk is null
        -- or source row is more recent and differs from target table
        or (
                t.dw_version_ts  < s.dw_version_ts  and t.dw_hash_diff  != s.dw_hash_diff
           )
) s
on
(
    t.dw_site_item_inventory_snapshot_shk = s.dw_site_item_inventory_snapshot_shk
)
when matched then update set
     t.dw_site_item_shk                    = s.dw_site_item_shk                    
    ,t.dw_hash_diff                        = s.dw_hash_diff                        
    ,t.dw_version_ts                       = s.dw_version_ts                       
    --                                    
    ,t.ndc                                 = s.ndc                                 
    --                                    
    ,t.qoh_date                            = s.qoh_date                            
    ,t.qoh                                 = s.qoh                                 
    ,t.reorder_point                       = s.reorder_point                       
    ,t.reorder_quantity                    = s.reorder_quantity                    
    ,t.unit_cost                           = s.unit_cost                           
    --    
    ,t.dw_json_index_no                    = s.dw_json_index_no                                
    ,t.dw_update_ts                        = s.dw_update_ts                        
when not matched then insert
(
     dw_site_item_inventory_snapshot_shk
    ,dw_site_item_shk                   
    ,dw_hash_diff                       
    ,dw_version_ts                      
    --                                  
    ,ndc                                
    --                                  
    ,qoh_date                           
    ,qoh                                
    ,reorder_point                      
    ,reorder_quantity                   
    ,unit_cost                          
    --
    ,dw_json_index_no
    ,dw_load_ts                         
    ,dw_update_ts                       
)
values
(
     s.dw_site_item_inventory_snapshot_shk
    ,s.dw_site_item_shk                   
    ,s.dw_hash_diff                       
    ,s.dw_version_ts                      
    --                                  
    ,s.ndc                                
    --                                  
    ,s.qoh_date                           
    ,s.qoh                                
    ,s.reorder_point                      
    ,s.reorder_quantity                   
    ,s.unit_cost                          
    --
    ,s.dw_json_index_no
    ,s.dw_load_ts                         
    ,s.dw_update_ts                       
)
;