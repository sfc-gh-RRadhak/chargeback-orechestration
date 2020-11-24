

insert overwrite into 
    {environment}_plt_pl_db.main.cb_resource_dm
select 
     brms.dw_resource_shk
    --
    ,brms.be_name           
    ,crh.organization_name 
    ,crh.account_name    
    ,crh.region_name
    ,crh.resource_name     
    ,crh.resource_type_cd  
    ,brms.tag_json:team::string     as team_name         
    ,brms.tag_json:env::string      as env_type_cd       
    ,brms.tag_json:gl_acct::string  as gl_account_str    
    --
    ,current_timestamp()            as dw_load_ts
from 
    {environment}_plt_il_db.main.be_resource_mapping_s brms
    join {environment}_plt_il_db.main.cb_resource_h crh on
        crh.dw_resource_shk = brms.dw_resource_shk
order by
    2,3,4,5
;