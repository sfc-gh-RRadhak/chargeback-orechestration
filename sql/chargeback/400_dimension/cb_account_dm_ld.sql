


insert overwrite into 
    {environment}_plt_pl_db.main.cb_account_dm
select distinct
     carl.dw_account_shk
    --
    ,brms.be_name           
    ,crh.organization_name 
    ,crh.account_name    
    ,crh.region_name
    --
    ,current_timestamp()            as dw_load_ts
from 
    {environment}_plt_il_db.main.be_resource_mapping_s brms
    join {environment}_plt_il_db.main.cb_resource_h crh on
        crh.dw_resource_shk = brms.dw_resource_shk
    join {environment}_plt_il_db.main.cb_account_resource_l carl on
        carl.dw_resource_shk = crh.dw_resource_shk
order by
    2,3,4,5
;



 

