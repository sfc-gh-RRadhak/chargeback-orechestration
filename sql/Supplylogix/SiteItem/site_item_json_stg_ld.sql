use role     {role};
use database {database};
use schema   {schema};
 

--------------------------------------------------------------------
-- truncate stage prior to bulk load
--
truncate table {database}.{schema}.site_item_json_stg;

--------------------------------------------------------------------
-- load all staged files
--
copy into
    {database}.{schema}.site_item_json_stg
from
    (
    select
         $1:NDC::string          
        ,$1::variant                                          
        ,metadata$filename                               -- dw_file_name
        ,metadata$file_row_number                        -- dw_file_row_no
        ,current_timestamp()                             -- dw_load_ts
    from
        @{stage}/{stage_file_path_pattern}/
    )
pattern =  '.*/\\d\\d\\d\\d/\\d\\d/\\d\\d/[0-9]/*.*site-items-extended*/*.*site_items.json.*' 
file_format   = ( format_name = '{format_name}' );