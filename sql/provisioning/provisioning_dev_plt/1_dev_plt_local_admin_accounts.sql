 
      
    use role securityadmin;
    
    create role if not exists dev_plt_securityadmin_fr ;

    -- set properties with an alter statement to streamline maintenance
    alter role dev_plt_securityadmin_fr
    set 
        comment = 'Local securityadmin role.';

    -- grant role to securityadmin
    grant role dev_plt_securityadmin_fr to role securityadmin;
    

    use role securityadmin;
    
    create or replace role   dev_plt_sysadmin_fr comment = 'Local sysadmin role.'  ;
    
    -- transfer ownership to local role  
    grant ownership
        on role dev_plt_sysadmin_fr
        to role dev_plt_securityadmin_fr;
    
     
    -- finish setup with local role to ensure ownership was transferred successfully
    use role dev_plt_securityadmin_fr;
     
    -- grant role to sysadmin
    grant role dev_plt_sysadmin_fr 
    to role sysadmin ;
    