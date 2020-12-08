USE DATABASE {database};
USE SCHEMA {schema}; 

DELETE FROM  TGT_QSX_TABLES.dw_countries;

INSERT INTO TGT_QSX_TABLES.dw_countries
(
    cntry_id,curncy_id,cntry_desc ,
    cntry_code ,iso_cntry_code ,cultural ,
    cntry_busn_unit ,high_vol_cntry_yn_id ,
    check_sil ,rev_rollup_id,rev_rollup ,prft_cntr_id,
    prft_cntr ,cre_date,upd_date,cre_user,upd_user
)

SELECT
    raw_data:cntry_id::DECIMAL(4,0),
    raw_data:curncy_id::DECIMAL(4,0),
    raw_data:cntry_desc::STRING,
    raw_data:cntry_code::STRING ,
    raw_data:iso_cntry_code::STRING,
    raw_data:cultural::STRING,
    raw_data:cntry_busn_unit::STRING,
    raw_data:high_vol_cntry_yn_id::TINYINT,
    raw_data:check_sil::TINYINT,
    raw_data:rev_rollup_id::SMALLINT,
    raw_data:rev_rollup::STRING,
    raw_data:prft_cntr_id::INT,
    raw_data:prft_cntr::STRING,
    raw_data:cre_date::DATE,
    raw_data:upd_date::TIMESTAMP,
    raw_data:cre_user::STRING,
    raw_data:upd_user::STRING
FROM {schema}.STG_COUNTRIES_W;