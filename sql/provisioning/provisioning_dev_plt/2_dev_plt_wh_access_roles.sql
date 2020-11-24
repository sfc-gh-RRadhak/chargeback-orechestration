-- Warehouse object creation and access roles
----------------WH Creation Script Start: dev_plt_adhoc_all_wh----------------
use role sysadmin; 
 create warehouse if not exists dev_plt_adhoc_all_wh 
 with 
   initially_suspended   = true; 
 grant ownership 
   on warehouse dev_plt_adhoc_all_wh 
   to role dev_plt_sysadmin_fr 
   revoke current grants   
 ; 
 use role dev_plt_sysadmin_fr;  
 alter warehouse dev_plt_adhoc_all_wh
 set 
   warehouse_size='xsmall' 
   auto_suspend= 60 
   comment='Warehouse for all users within the business business_entity';
    ------------------WH Access Roles Start dev_plt_adhoc_all_wh_operate_ar------------------


    use role securityadmin;
    create role if not exists dev_plt_adhoc_all_wh_operate_ar comment='Local access role.' ;
    grant ownership on role dev_plt_adhoc_all_wh_operate_ar to role dev_plt_securityadmin_fr revoke current grants;
    use role dev_plt_securityadmin_fr;
    grant role dev_plt_adhoc_all_wh_operate_ar to role dev_plt_sysadmin_fr; 
    use role dev_plt_sysadmin_fr;
    grant monitor, usage, operate, modify on warehouse dev_plt_adhoc_all_wh to role dev_plt_adhoc_all_wh_operate_ar;

    ------------------WH Access Roles End dev_plt_adhoc_all_wh_operate_ar------------------

    ------------------WH Access Roles Start dev_plt_adhoc_all_wh_use_ar------------------


    use role securityadmin;
    create role if not exists dev_plt_adhoc_all_wh_use_ar comment='Local access role.' ;
    grant ownership on role dev_plt_adhoc_all_wh_use_ar to role dev_plt_securityadmin_fr revoke current grants;
    use role dev_plt_securityadmin_fr;
    grant role dev_plt_adhoc_all_wh_use_ar to role dev_plt_sysadmin_fr; 
    use role dev_plt_sysadmin_fr;
    grant monitor, usage on warehouse dev_plt_adhoc_all_wh to role dev_plt_adhoc_all_wh_use_ar;

    ------------------WH Access Roles End dev_plt_adhoc_all_wh_use_ar------------------

----------------WH  Creation Script End: dev_plt_adhoc_all_wh----------------

----------------WH Creation Script Start: dev_plt_adhoc_analyst_wh----------------
use role sysadmin; 
 create warehouse if not exists dev_plt_adhoc_analyst_wh 
 with 
   initially_suspended   = true; 
 grant ownership 
   on warehouse dev_plt_adhoc_analyst_wh 
   to role dev_plt_sysadmin_fr 
   revoke current grants   
 ; 
 use role dev_plt_sysadmin_fr;  
 alter warehouse dev_plt_adhoc_analyst_wh
 set 
   warehouse_size='xsmall' 
   auto_suspend= 60 
   comment='Warehouse for the analyst team';
    ------------------WH Access Roles Start dev_plt_adhoc_analyst_wh_operate_ar------------------


    use role securityadmin;
    create role if not exists dev_plt_adhoc_analyst_wh_operate_ar comment='Local access role.' ;
    grant ownership on role dev_plt_adhoc_analyst_wh_operate_ar to role dev_plt_securityadmin_fr revoke current grants;
    use role dev_plt_securityadmin_fr;
    grant role dev_plt_adhoc_analyst_wh_operate_ar to role dev_plt_sysadmin_fr; 
    use role dev_plt_sysadmin_fr;
    grant monitor, usage, operate, modify on warehouse dev_plt_adhoc_analyst_wh to role dev_plt_adhoc_analyst_wh_operate_ar;

    ------------------WH Access Roles End dev_plt_adhoc_analyst_wh_operate_ar------------------

    ------------------WH Access Roles Start dev_plt_adhoc_analyst_wh_use_ar------------------


    use role securityadmin;
    create role if not exists dev_plt_adhoc_analyst_wh_use_ar comment='Local access role.' ;
    grant ownership on role dev_plt_adhoc_analyst_wh_use_ar to role dev_plt_securityadmin_fr revoke current grants;
    use role dev_plt_securityadmin_fr;
    grant role dev_plt_adhoc_analyst_wh_use_ar to role dev_plt_sysadmin_fr; 
    use role dev_plt_sysadmin_fr;
    grant monitor, usage on warehouse dev_plt_adhoc_analyst_wh to role dev_plt_adhoc_analyst_wh_use_ar;

    ------------------WH Access Roles End dev_plt_adhoc_analyst_wh_use_ar------------------

----------------WH  Creation Script End: dev_plt_adhoc_analyst_wh----------------

----------------WH Creation Script Start: dev_plt_adhoc_datascience_wh----------------
use role sysadmin; 
 create warehouse if not exists dev_plt_adhoc_datascience_wh 
 with 
   initially_suspended   = true; 
 grant ownership 
   on warehouse dev_plt_adhoc_datascience_wh 
   to role dev_plt_sysadmin_fr 
   revoke current grants   
 ; 
 use role dev_plt_sysadmin_fr;  
 alter warehouse dev_plt_adhoc_datascience_wh
 set 
   warehouse_size='xsmall' 
   auto_suspend= 60 
   comment='Warehouse for the data science team';
    ------------------WH Access Roles Start dev_plt_adhoc_datascience_wh_operate_ar------------------


    use role securityadmin;
    create role if not exists dev_plt_adhoc_datascience_wh_operate_ar comment='Local access role.' ;
    grant ownership on role dev_plt_adhoc_datascience_wh_operate_ar to role dev_plt_securityadmin_fr revoke current grants;
    use role dev_plt_securityadmin_fr;
    grant role dev_plt_adhoc_datascience_wh_operate_ar to role dev_plt_sysadmin_fr; 
    use role dev_plt_sysadmin_fr;
    grant monitor, usage, operate, modify on warehouse dev_plt_adhoc_datascience_wh to role dev_plt_adhoc_datascience_wh_operate_ar;

    ------------------WH Access Roles End dev_plt_adhoc_datascience_wh_operate_ar------------------

    ------------------WH Access Roles Start dev_plt_adhoc_datascience_wh_use_ar------------------


    use role securityadmin;
    create role if not exists dev_plt_adhoc_datascience_wh_use_ar comment='Local access role.' ;
    grant ownership on role dev_plt_adhoc_datascience_wh_use_ar to role dev_plt_securityadmin_fr revoke current grants;
    use role dev_plt_securityadmin_fr;
    grant role dev_plt_adhoc_datascience_wh_use_ar to role dev_plt_sysadmin_fr; 
    use role dev_plt_sysadmin_fr;
    grant monitor, usage on warehouse dev_plt_adhoc_datascience_wh to role dev_plt_adhoc_datascience_wh_use_ar;

    ------------------WH Access Roles End dev_plt_adhoc_datascience_wh_use_ar------------------

----------------WH  Creation Script End: dev_plt_adhoc_datascience_wh----------------

----------------WH Creation Script Start: dev_plt_service_elt_load_wh----------------
use role sysadmin; 
 create warehouse if not exists dev_plt_service_elt_load_wh 
 with 
   initially_suspended   = true; 
 grant ownership 
   on warehouse dev_plt_service_elt_load_wh 
   to role dev_plt_sysadmin_fr 
   revoke current grants   
 ; 
 use role dev_plt_sysadmin_fr;  
 alter warehouse dev_plt_service_elt_load_wh
 set 
   warehouse_size='medium' 
   auto_suspend= 60 
   comment='Warehouse used for loading with copy into';
    ------------------WH Access Roles Start dev_plt_service_elt_load_wh_operate_ar------------------


    use role securityadmin;
    create role if not exists dev_plt_service_elt_load_wh_operate_ar comment='Local access role.' ;
    grant ownership on role dev_plt_service_elt_load_wh_operate_ar to role dev_plt_securityadmin_fr revoke current grants;
    use role dev_plt_securityadmin_fr;
    grant role dev_plt_service_elt_load_wh_operate_ar to role dev_plt_sysadmin_fr; 
    use role dev_plt_sysadmin_fr;
    grant monitor, usage, operate, modify on warehouse dev_plt_service_elt_load_wh to role dev_plt_service_elt_load_wh_operate_ar;

    ------------------WH Access Roles End dev_plt_service_elt_load_wh_operate_ar------------------

    ------------------WH Access Roles Start dev_plt_service_elt_load_wh_use_ar------------------


    use role securityadmin;
    create role if not exists dev_plt_service_elt_load_wh_use_ar comment='Local access role.' ;
    grant ownership on role dev_plt_service_elt_load_wh_use_ar to role dev_plt_securityadmin_fr revoke current grants;
    use role dev_plt_securityadmin_fr;
    grant role dev_plt_service_elt_load_wh_use_ar to role dev_plt_sysadmin_fr; 
    use role dev_plt_sysadmin_fr;
    grant monitor, usage on warehouse dev_plt_service_elt_load_wh to role dev_plt_service_elt_load_wh_use_ar;

    ------------------WH Access Roles End dev_plt_service_elt_load_wh_use_ar------------------

----------------WH  Creation Script End: dev_plt_service_elt_load_wh----------------

----------------WH Creation Script Start: dev_plt_service_elt_trans_wh----------------
use role sysadmin; 
 create warehouse if not exists dev_plt_service_elt_trans_wh 
 with 
   initially_suspended   = true; 
 grant ownership 
   on warehouse dev_plt_service_elt_trans_wh 
   to role dev_plt_sysadmin_fr 
   revoke current grants   
 ; 
 use role dev_plt_sysadmin_fr;  
 alter warehouse dev_plt_service_elt_trans_wh
 set 
   warehouse_size='medium' 
   auto_suspend= 60 
   comment='Warehouse used for transformation workloads';
    ------------------WH Access Roles Start dev_plt_service_elt_trans_wh_operate_ar------------------


    use role securityadmin;
    create role if not exists dev_plt_service_elt_trans_wh_operate_ar comment='Local access role.' ;
    grant ownership on role dev_plt_service_elt_trans_wh_operate_ar to role dev_plt_securityadmin_fr revoke current grants;
    use role dev_plt_securityadmin_fr;
    grant role dev_plt_service_elt_trans_wh_operate_ar to role dev_plt_sysadmin_fr; 
    use role dev_plt_sysadmin_fr;
    grant monitor, usage, operate, modify on warehouse dev_plt_service_elt_trans_wh to role dev_plt_service_elt_trans_wh_operate_ar;

    ------------------WH Access Roles End dev_plt_service_elt_trans_wh_operate_ar------------------

    ------------------WH Access Roles Start dev_plt_service_elt_trans_wh_use_ar------------------


    use role securityadmin;
    create role if not exists dev_plt_service_elt_trans_wh_use_ar comment='Local access role.' ;
    grant ownership on role dev_plt_service_elt_trans_wh_use_ar to role dev_plt_securityadmin_fr revoke current grants;
    use role dev_plt_securityadmin_fr;
    grant role dev_plt_service_elt_trans_wh_use_ar to role dev_plt_sysadmin_fr; 
    use role dev_plt_sysadmin_fr;
    grant monitor, usage on warehouse dev_plt_service_elt_trans_wh to role dev_plt_service_elt_trans_wh_use_ar;

    ------------------WH Access Roles End dev_plt_service_elt_trans_wh_use_ar------------------

----------------WH  Creation Script End: dev_plt_service_elt_trans_wh----------------
