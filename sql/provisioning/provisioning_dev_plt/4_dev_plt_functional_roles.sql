-----  Functional Roles------ 
        use role securityadmin;

        -- keep create functional role
        create role if not exists dev_plt_public_fr;
        -- transfer ownership to local role
        grant ownership
            on role dev_plt_public_fr
            to role dev_plt_securityadmin_fr  
        ;
        
        use role dev_plt_securityadmin_fr  ;
 ------------- Functional Access Role to Database dev_plt_dl_source_db---------------
         grant role dev_plt_dl_source_db_ro_ar
            to role dev_plt_public_fr;
 ------------- Functional Access Role to Database dev_plt_edw_db---------------
         grant role dev_plt_edw_db_ro_ar
            to role dev_plt_public_fr; 
 ------------- Functional Access Role to Database dev_plt_dl_source_db---------------
         grant role dev_plt_dl_source_db_ro_ar
            to role dev_plt_public_fr;
 ------------- Functional Access Role to Warehouse dev_plt_adhoc_analyst_wh---------------
         grant role dev_plt_adhoc_analyst_wh_use_ar
            to role dev_plt_public_fr; 
 ------------- Functional Access Role to Warehouse dev_plt_adhoc_all_wh---------------
         grant role dev_plt_adhoc_all_wh_operate_ar
            to role dev_plt_public_fr; 
