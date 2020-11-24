 copy into
    @{stage}/{load_name}/{fileName}
from
(
    select
         'MCKESSON'             as organization_name
        ,current_account()      as account_name
        ,current_region()       as region_name
        ,*
    from
        snowflake.ACCOUNT_USAGE.login_history    where EVENT_TIMESTAMP>=dateadd( minutes, -{minutes}, current_timestamp())  
    order by
        event_timestamp
)
file_format      = ( type=csv field_optionally_enclosed_by = '"' )
overwrite        = false
single           = false
include_query_id = true
detailed_output  = true
;
 

