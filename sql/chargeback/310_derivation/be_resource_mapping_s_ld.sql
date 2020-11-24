insert overwrite into {environment}_plt_il_db.main.be_resource_mapping_s
with l_mapping as
(
    select
        row_number() over( partition by crh.dw_resource_shk order by brml.priority_no ) as seq_no
       ,crh.dw_resource_shk
       ,brml.tag_json:be::string        as be_name
       ,brml.priority_no
       ,brml.match_pattern
       ,brml.tag_json
    from
        {environment}_plt_il_db.main.cb_resource_h crh
        join {environment}_plt_rl_snowflake_db.account_usage.be_resource_mapping_lkp brml on
            regexp_like( crh.resource_name, brml.match_pattern, 'i' )
)
select 
     lm.dw_resource_shk          
    ,lm.be_name
    ,lm.tag_json    
    ,lm.priority_no
    ,lm.match_pattern 
    ,current_timestamp()        as dw_load_ts
from 
    l_mapping lm
where 
    lm.seq_no = 1  
order by
    2,1
 
