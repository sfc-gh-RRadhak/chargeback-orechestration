insert into 
    {environment}_plt_il_db.main.cb_account_resource_l
select 
     cah.dw_account_shk
    ,crh.dw_resource_shk
    ,current_timestamp()            as dw_load_ts
    ,current_timestamp()            as dw_update_ts
from
     {environment}_plt_il_db.main.cb_resource_h crh
     join {environment}_plt_il_db.main.cb_account_h cah on
             cah.organization_name = crh.organization_name
         and cah.account_name      = crh.account_name
         and cah.region_name       = crh.region_name
where
    (cah.dw_account_shk, crh.dw_resource_shk) not in
    (
        select dw_account_shk, dw_resource_shk from {environment}_plt_il_db.main.cb_account_resource_l
    )
order by
    1,2
;