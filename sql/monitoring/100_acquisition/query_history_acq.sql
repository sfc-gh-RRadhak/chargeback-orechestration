copy into
     @{stage}/{load_name}/{fileName}
from
(
    select
         'MCKESSON'             as organization_name
        ,current_account()      as account_name
        ,current_region()       as region_name
        ,*
    from snowflake.ACCOUNT_USAGE.query_history where  END_TIME>=dateadd( minutes, -{minutes}, current_timestamp())  
    order by
        end_time
)
file_format      = ( type=csv field_optionally_enclosed_by = '"' )
overwrite        = false
single           = false
include_query_id = true
detailed_output  = true
 
 

