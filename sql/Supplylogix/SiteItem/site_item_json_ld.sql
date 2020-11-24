USE role     {role};
USE database {database};
USE schema   {schema};
 
--------------------------------------------------------------------
-- insert new and modified records into target table with version date
-- dedupe source records as part of insert
INSERT INTO  {database}.{schema}.site_item_json
with l_stg as
(
    select
         sha1_binary( s.ndc )                                           as dw_site_item_shk
        ,sha1_binary( concat( s.ndc
                             ,'|'
                             ,s.src_json ) 
                    )                                                   as dw_hash_diff
        ,s.dw_load_ts                                                   as dw_version_ts
        --
        ,s.ndc
        ,s.src_json:NDCNumeric::string                                  as ndc_numeric
        ,s.src_json:NPI::string                                         as npi
        ,s.src_json:SiteId::string                                      as site_id
        ,s.src_json:DispenseDetail::variant                             as dispense_detail_json
        ,s.src_json:InventoryDetail::variant                            as inventory_detail_json
        ,s.src_json:SiteItem::variant                                   as site_item_json
        --
        ,dw_file_name
        ,dw_file_row_no
        ,current_timestamp()                                            as dw_load_ts
    from
        {database}.{schema}.site_item_json_stg s
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
     s.dw_site_item_shk
    ,s.dw_hash_diff
    ,s.dw_version_ts
    --
    ,s.ndc
    ,s.ndc_numeric
    ,s.npi
    ,s.site_id
    ,s.dispense_detail_json
    ,s.inventory_detail_json
    ,s.site_item_json
    --
    ,s.dw_file_name
    ,s.dw_file_row_no
    ,s.dw_load_ts
from
    l_deduped s
where
    s.dw_hash_diff not in
    (
        select dw_hash_diff from site_item_json
    )
order by
    s.ndc
;