use role     {role};
use database {database};
use schema   {schema};

 
--------------------------------------------------------------------
-- insert new and modified records into target table with version date
-- dedupe source records as part of insert
--
insert into 
    {database}.{schema}.chain_item_json
with l_stg as
(
    select
         sha1_binary( concat( s.ndc
                             ,'|'
                             ,si.key::string ) 
                    )                                                   as dw_chain_item_shk
        ,sha1_binary( concat( s.ndc
                             ,'|'
                             ,si.key::string
                             ,'|'
                             ,si.value ) 
                    )                                                   as dw_hash_diff
        ,s.dw_load_ts                                                   as dw_version_ts
        --
        ,s.ndc
        ,s.src_json:IsTransferable::string                              as is_transferable
        ,s.src_json:PackSize::number                                    as pack_size
        ,s.src_json:SuggestedReorderQuantity::number                    as suggested_reorder_quantity
        ,s.src_json:UnitCost::float                                     as unit_cost
        ,si.key::string                                                 as item_no
        ,si.value::variant                                              as item_json
        --
        ,dw_file_name
        ,dw_file_row_no
        ,current_timestamp()                                            as dw_load_ts
    from
         {database}.{schema}.chain_item_json_stg s
        ,lateral flatten( input => s.src_json:Items, outer => true ) si
)
,l_deduped as
(
    select
        *
    from
        (
        select
             -- identify dupes and only keep copy 1
             row_number() over( partition by dw_hash_diff order by 1 ) as seq_no
            ,s.*
        from
            l_stg s
        )
    where
        seq_no = 1 -- keep only unique rows
)
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
    ,s.item_json
    --
    ,s.dw_file_name
    ,s.dw_file_row_no
    ,s.dw_load_ts
from
    l_deduped s
where
    s.dw_hash_diff not in
    (
        select dw_hash_diff from chain_item_json
    )
order by
      s.ndc
     ,s.item_no
;