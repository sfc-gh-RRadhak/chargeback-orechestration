USE DATABASE {database};
USE SCHEMA {schema}; 
DELETE FROM DEV_FIN_DB.STG_QSX_TABLES.STG_COUNTRIES_W;
copy into {database}.{schema}.STG_COUNTRIES_W (file_name,file_row_number,raw_data)
from (
SELECT
metadata$filename,
metadata$file_row_number,*
FROM
  @{stage}/dw_countries/{stage_file_path_pattern}
  (file_format => '{fileformat}')
) force = true;

