

-- mapping match patterns and priorities
insert overwrite into {environment}_plt_rl_snowflake_db.account_usage.be_resource_mapping_lkp
with l_mapping as
(
    select to_char( null ) as match_pattern, to_number( null ) as priority_no, to_variant( null ) as tag_json
    -- D&A data management
    union all select 'prd_ibmvaricent_db',                          0, object_construct( 'be','d&a','env','prd','team','all','gl_acct','xxxxx.xxxxx' )  
    union all select 'ibmvaricent_wh',                              0, object_construct( 'be','d&a','env','prd','team','all','gl_acct','xxxxx.xxxxx' )  
    union all select 'dstage_ingestion_wh',                         0, object_construct( 'be','d&a','env','prd','team','all','gl_acct','xxxxx.xxxxx' )  
    union all select 'prd_(ent|mckedw|mck|dl|data_lake|extrnl)_.*', 2, object_construct( 'be','d&a','env','prd','team','all','gl_acct','xxxxx.xxxxx' )  
    union all select 'uat_(ent|mckedw|mck|dl|data_lake|extrnl)_.*', 2, object_construct( 'be','d&a','env','uat','team','all','gl_acct','xxxxx.xxxxx' )  
    union all select 'qat_(ent|mckedw|mck|dl|data_lake|extrnl)_.*', 2, object_construct( 'be','d&a','env','qat','team','all','gl_acct','xxxxx.xxxxx' )  
    union all select 'dev_(ent|mckedw|mck|dl|data_lake|extrnl)_.*', 2, object_construct( 'be','d&a','env','dev','team','all','gl_acct','xxxxx.xxxxx' )  
    -- mls
    union all select 'prd_mls_.*_ds_[^_]+', 1, object_construct( 'be','mls','env','prd','team','data science','gl_acct','xxxxx.xxxxx' )
    union all select 'prd_mls_.*',          2, object_construct( 'be','mls','env','prd','team','all','gl_acct','xxxxx.xxxxx' )
    union all select 'uat_mls_.*',          2, object_construct( 'be','mls','env','uat','team','all','gl_acct','xxxxx.xxxxx' )
    union all select 'dev_mls_.*',          2, object_construct( 'be','mls','env','dev','team','all','gl_acct','xxxxx.xxxxx' )
    -- psas
    union all select 'prd_psas_.*_ds_[^_]+', 1, object_construct( 'be','psas','env','prd','team','data science','gl_acct','xxxxx.xxxxx' )
    union all select 'prd_psas_.*',          2, object_construct( 'be','psas','env','prd','team','all','gl_acct','xxxxx.xxxxx' )
    union all select 'sbx_psas_.*',          2, object_construct( 'be','psas','env','prd','team','all','gl_acct','xxxxx.xxxxx' )
    union all select 'uat_psas_.*',          2, object_construct( 'be','psas','env','uat','team','all','gl_acct','xxxxx.xxxxx' )
    union all select 'qat_psas_.*',          2, object_construct( 'be','psas','env','qat','team','all','gl_acct','xxxxx.xxxxx' )
    union all select 'qat_[^_]*_psas_.*',    2, object_construct( 'be','psas','env','qat','team','all','gl_acct','xxxxx.xxxxx' )
    union all select 'dev_psas_.*',          2, object_construct( 'be','psas','env','dev','team','all','gl_acct','xxxxx.xxxxx' )
    --CRNA/ERA
    union all select 'dev_crna_.*',          2, object_construct( 'be','crna','env','dev','team','all','gl_acct','xxxxx.xxxxx' )
    union all select 'uat_crna_.*',          2, object_construct( 'be','crna','env','uat','team','all','gl_acct','xxxxx.xxxxx' )
    union all select 'prd_crna_.*',          2, object_construct( 'be','crna','env','prd','team','all','gl_acct','xxxxx.xxxxx' )
    -- default
    union all select '.*', 5, object_construct( 'be','?','env','?','team','?','gl_acct','xxxxx.xxxxx' )  
)
select
     lm.match_pattern
    ,lm.priority_no
    ,lm.tag_json
    ,current_timestamp()    as dw_load_ts
from
    l_mapping lm
where
    lm.tag_json is not null
;

 
with l_resource as
(
--    select distinct warehouse_name as resource_name
--    from   snowflake.account_usage.warehouse_metering_history wmh
--    where  wmh.end_time >= dateadd( day, -7, current_date() )
    select
        $1 as resource_name
    from
    ( values
        -- ent
         ( 'prd_ibmvaricent_db' )
        ,( 'dstage_ingestion_wh' )
        ,( 'prd_ent_xxx_wh' )
        ,( 'prd_mckedw_xxx_db' )
        ,( 'dev_mck_xxx_wh' )
        ,( 'uat_dl_xxx_db' )
        ,( 'qat_data_lake_xxx_db' )
        ,( 'qat_extrnl_xxx_db' )
        -- mls
        ,( 'prd_mls_xxx_wh' )
        ,( 'prd_mls_xxx_yyy_ds_wh' )
        ,( 'uat_mls_xxx_wh' )
        ,( 'dev_mls_xxx_db' )
        -- psas
        ,( 'prd_psas_xxx_wh' )
        ,( 'prd_psas_xxx_yyy_zzzz_ds_wh' )
        ,( 'uat_psas_xxx_wh' )
        ,( 'qat_psas_xxx_wh' )
        ,( 'qat_xxx_psas_db')
        ,( 'dev_psas_xxx_db' )
        -- default
        ,( 'plt_looker_wh' )
    )
)
,l_mapping as
(
    select
        row_number() over( partition by lr.resource_name order by brml.priority_no ) as seq_no
       ,brml.tag_json:be::string        as be_name
       ,brml.tag_json:env::string       as env_type_cd
       ,brml.tag_json:team::string      as team_name
       ,lr.resource_name
       ,brml.priority_no
       ,brml.match_pattern
       ,brml.tag_json
    from
        l_resource lr
        join {environment}_plt_rl_snowflake_db.account_usage.be_resource_mapping_lkp brml on
            regexp_like( lr.resource_name, brml.match_pattern, 'i' )
)
select 
     lm.* 
    ,count( distinct lm.resource_name ) over() as resource_cnt
from 
    l_mapping lm
where 
    lm.seq_no = 1 -- highest priority match
order by
     lm.be_name
    ,lm.env_type_cd
    ,lm.resource_name
;
