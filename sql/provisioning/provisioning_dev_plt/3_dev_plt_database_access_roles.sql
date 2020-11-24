------------------------- Database & Access Roles Creation -------------------------

        use role sysadmin; 
        create database if not exists dev_plt_wrk_all_db  comment='Workspace database for all users within the business business_entity';
        -- transfer ownership to local role
        grant ownership
            on database dev_plt_wrk_all_db
            to role dev_plt_sysadmin_fr
            revoke current grants
        ;
        grant ownership
            on all schemas in database dev_plt_wrk_all_db
            to role dev_plt_sysadmin_fr
            revoke current grants
        ;
        use role dev_plt_sysadmin_fr;
        -- drop public schema since it should never be used.
        drop schema dev_plt_wrk_all_db.public;

        -- creating schema 
        create schema if not exists dev_plt_wrk_all_db.main   with managed access;
        
        Alter schema   dev_plt_wrk_all_db.main Set comment='Schema for general use.'  ;
        
        -- access roles for database & schemas

        
        use role securityadmin;
        -- keep create statement minimal
        create role if not exists dev_plt_wrk_all_db_rw_ar comment = 'Local access role.';
        -- transfer ownership to local role
        grant ownership
            on role dev_plt_wrk_all_db_rw_ar 
            to role dev_plt_securityadmin_fr    
            revoke current grants;
        use role dev_plt_securityadmin_fr;   
        -- grant object privileges with local role
        use role dev_plt_sysadmin_fr;
        
        -- grant database privileges to the access role
        grant monitor, usage
                on database dev_plt_wrk_all_db
                to role dev_plt_wrk_all_db_rw_ar;

        -- grant schema privileges to the access role
        grant monitor, usage
                on all schemas in database dev_plt_wrk_all_db
                to role dev_plt_wrk_all_db_rw_ar;
 

        grant create table, create view, create file format, create stage, create sequence, create function, create procedure
                on all schemas in database dev_plt_wrk_all_db
                to role dev_plt_wrk_all_db_rw_ar;


        -- grant schema object privileges to the access role, repeat for each schema
                
        grant select, insert, update, delete, truncate
                on all tables in schema dev_plt_wrk_all_db.main
                to role dev_plt_wrk_all_db_rw_ar;

        grant select, insert, update, delete, truncate
                on future tables in schema dev_plt_wrk_all_db.main
                to role dev_plt_wrk_all_db_rw_ar;
                
        
        use role securityadmin;
        -- keep create statement minimal
        create role if not exists dev_plt_wrk_all_db_ro_ar comment = 'Local access role.';
        -- transfer ownership to local role
        grant ownership
            on role dev_plt_wrk_all_db_ro_ar 
            to role dev_plt_securityadmin_fr    
            revoke current grants;
        use role dev_plt_securityadmin_fr;   
        -- grant object privileges with local role
        use role dev_plt_sysadmin_fr;
         
        -- grant database privileges to the access role
        grant monitor, usage
            on database dev_plt_wrk_all_db
            to role dev_plt_wrk_all_db_ro_ar;
        -- grant schema privileges to the access role
        grant monitor, usage
            on all schemas in database dev_plt_wrk_all_db
            to role dev_plt_wrk_all_db_ro_ar;
        -- grant schema object privileges to the access role, repeat for each schema
       
        grant select
            on all tables in schema dev_plt_wrk_all_db.main
            to role dev_plt_wrk_all_db_ro_ar;
        grant select
            on future tables in schema dev_plt_wrk_all_db.main
            to role dev_plt_wrk_all_db_ro_ar;
        
        use role securityadmin;
        -- keep create statement minimal
        create role if not exists dev_plt_wrk_all_db_ref_ar comment = 'Local access role.';
        -- transfer ownership to local role
        grant ownership
            on role dev_plt_wrk_all_db_ref_ar 
            to role dev_plt_securityadmin_fr    
            revoke current grants;
        use role dev_plt_securityadmin_fr;   
        -- grant object privileges with local role
        use role dev_plt_sysadmin_fr;
         
        -- grant database privileges to the access role
        grant monitor, usage
            on database dev_plt_wrk_all_db
            to role dev_plt_wrk_all_db_ref_ar;
        -- grant schema privileges to the access role
        grant monitor, usage
            on all schemas in database dev_plt_wrk_all_db
            to role dev_plt_wrk_all_db_ref_ar;
        -- grant schema object privileges to the access role, repeat for each schema
        grant references
            on all tables in schema dev_plt_wrk_all_db.main
            to role dev_plt_wrk_all_db_ref_ar;
        grant references
            on future tables in schema dev_plt_wrk_all_db.main
            to role dev_plt_wrk_all_db_ref_ar;
        
        use role sysadmin; 
        create database if not exists dev_plt_wrk_datascience_db  comment='Workspace database for the data science team';
        -- transfer ownership to local role
        grant ownership
            on database dev_plt_wrk_datascience_db
            to role dev_plt_sysadmin_fr
            revoke current grants
        ;
        grant ownership
            on all schemas in database dev_plt_wrk_datascience_db
            to role dev_plt_sysadmin_fr
            revoke current grants
        ;
        use role dev_plt_sysadmin_fr;
        -- drop public schema since it should never be used.
        drop schema dev_plt_wrk_datascience_db.public;

        -- creating schema 
        create schema if not exists dev_plt_wrk_datascience_db.main   with managed access;
        create schema if not exists dev_plt_wrk_datascience_db.jsimth   with managed access;
        create schema if not exists dev_plt_wrk_datascience_db.jjones   with managed access;
        
        Alter schema   dev_plt_wrk_datascience_db.main Set comment='Schema for general use.'  ;
        Alter schema   dev_plt_wrk_datascience_db.jsimth Set comment='Schema for general use.'  ;
        Alter schema   dev_plt_wrk_datascience_db.jjones Set comment='Schema for general use.'  ;
        
        -- access roles for database & schemas

        
        use role securityadmin;
        -- keep create statement minimal
        create role if not exists dev_plt_wrk_datascience_db_rw_ar comment = 'Local access role.';
        -- transfer ownership to local role
        grant ownership
            on role dev_plt_wrk_datascience_db_rw_ar 
            to role dev_plt_securityadmin_fr    
            revoke current grants;
        use role dev_plt_securityadmin_fr;   
        -- grant object privileges with local role
        use role dev_plt_sysadmin_fr;
        
        -- grant database privileges to the access role
        grant monitor, usage
                on database dev_plt_wrk_datascience_db
                to role dev_plt_wrk_datascience_db_rw_ar;

        -- grant schema privileges to the access role
        grant monitor, usage
                on all schemas in database dev_plt_wrk_datascience_db
                to role dev_plt_wrk_datascience_db_rw_ar;
 

        grant create table, create view, create file format, create stage, create sequence, create function, create procedure
                on all schemas in database dev_plt_wrk_datascience_db
                to role dev_plt_wrk_datascience_db_rw_ar;


        -- grant schema object privileges to the access role, repeat for each schema
                
        grant select, insert, update, delete, truncate
                on all tables in schema dev_plt_wrk_datascience_db.main
                to role dev_plt_wrk_datascience_db_rw_ar;

        grant select, insert, update, delete, truncate
                on future tables in schema dev_plt_wrk_datascience_db.main
                to role dev_plt_wrk_datascience_db_rw_ar;
                
        
        use role securityadmin;
        -- keep create statement minimal
        create role if not exists dev_plt_wrk_datascience_db_ro_ar comment = 'Local access role.';
        -- transfer ownership to local role
        grant ownership
            on role dev_plt_wrk_datascience_db_ro_ar 
            to role dev_plt_securityadmin_fr    
            revoke current grants;
        use role dev_plt_securityadmin_fr;   
        -- grant object privileges with local role
        use role dev_plt_sysadmin_fr;
         
        -- grant database privileges to the access role
        grant monitor, usage
            on database dev_plt_wrk_datascience_db
            to role dev_plt_wrk_datascience_db_ro_ar;
        -- grant schema privileges to the access role
        grant monitor, usage
            on all schemas in database dev_plt_wrk_datascience_db
            to role dev_plt_wrk_datascience_db_ro_ar;
        -- grant schema object privileges to the access role, repeat for each schema
       
        grant select
            on all tables in schema dev_plt_wrk_datascience_db.main
            to role dev_plt_wrk_datascience_db_ro_ar;
        grant select
            on future tables in schema dev_plt_wrk_datascience_db.main
            to role dev_plt_wrk_datascience_db_ro_ar;
        
        use role securityadmin;
        -- keep create statement minimal
        create role if not exists dev_plt_wrk_datascience_db_ref_ar comment = 'Local access role.';
        -- transfer ownership to local role
        grant ownership
            on role dev_plt_wrk_datascience_db_ref_ar 
            to role dev_plt_securityadmin_fr    
            revoke current grants;
        use role dev_plt_securityadmin_fr;   
        -- grant object privileges with local role
        use role dev_plt_sysadmin_fr;
         
        -- grant database privileges to the access role
        grant monitor, usage
            on database dev_plt_wrk_datascience_db
            to role dev_plt_wrk_datascience_db_ref_ar;
        -- grant schema privileges to the access role
        grant monitor, usage
            on all schemas in database dev_plt_wrk_datascience_db
            to role dev_plt_wrk_datascience_db_ref_ar;
        -- grant schema object privileges to the access role, repeat for each schema
        grant references
            on all tables in schema dev_plt_wrk_datascience_db.main
            to role dev_plt_wrk_datascience_db_ref_ar;
        grant references
            on future tables in schema dev_plt_wrk_datascience_db.main
            to role dev_plt_wrk_datascience_db_ref_ar;
        
        use role securityadmin;
        -- keep create statement minimal
        create role if not exists dev_plt_wrk_datascience_db_rw_ar comment = 'Local access role.';
        -- transfer ownership to local role
        grant ownership
            on role dev_plt_wrk_datascience_db_rw_ar 
            to role dev_plt_securityadmin_fr    
            revoke current grants;
        use role dev_plt_securityadmin_fr;   
        -- grant object privileges with local role
        use role dev_plt_sysadmin_fr;
        
        -- grant database privileges to the access role
        grant monitor, usage
                on database dev_plt_wrk_datascience_db
                to role dev_plt_wrk_datascience_db_rw_ar;

        -- grant schema privileges to the access role
        grant monitor, usage
                on all schemas in database dev_plt_wrk_datascience_db
                to role dev_plt_wrk_datascience_db_rw_ar;
 

        grant create table, create view, create file format, create stage, create sequence, create function, create procedure
                on all schemas in database dev_plt_wrk_datascience_db
                to role dev_plt_wrk_datascience_db_rw_ar;


        -- grant schema object privileges to the access role, repeat for each schema
                
        grant select, insert, update, delete, truncate
                on all tables in schema dev_plt_wrk_datascience_db.jsimth
                to role dev_plt_wrk_datascience_db_rw_ar;

        grant select, insert, update, delete, truncate
                on future tables in schema dev_plt_wrk_datascience_db.jsimth
                to role dev_plt_wrk_datascience_db_rw_ar;
                
        
        use role securityadmin;
        -- keep create statement minimal
        create role if not exists dev_plt_wrk_datascience_db_ro_ar comment = 'Local access role.';
        -- transfer ownership to local role
        grant ownership
            on role dev_plt_wrk_datascience_db_ro_ar 
            to role dev_plt_securityadmin_fr    
            revoke current grants;
        use role dev_plt_securityadmin_fr;   
        -- grant object privileges with local role
        use role dev_plt_sysadmin_fr;
         
        -- grant database privileges to the access role
        grant monitor, usage
            on database dev_plt_wrk_datascience_db
            to role dev_plt_wrk_datascience_db_ro_ar;
        -- grant schema privileges to the access role
        grant monitor, usage
            on all schemas in database dev_plt_wrk_datascience_db
            to role dev_plt_wrk_datascience_db_ro_ar;
        -- grant schema object privileges to the access role, repeat for each schema
       
        grant select
            on all tables in schema dev_plt_wrk_datascience_db.jsimth
            to role dev_plt_wrk_datascience_db_ro_ar;
        grant select
            on future tables in schema dev_plt_wrk_datascience_db.jsimth
            to role dev_plt_wrk_datascience_db_ro_ar;
        
        use role securityadmin;
        -- keep create statement minimal
        create role if not exists dev_plt_wrk_datascience_db_ref_ar comment = 'Local access role.';
        -- transfer ownership to local role
        grant ownership
            on role dev_plt_wrk_datascience_db_ref_ar 
            to role dev_plt_securityadmin_fr    
            revoke current grants;
        use role dev_plt_securityadmin_fr;   
        -- grant object privileges with local role
        use role dev_plt_sysadmin_fr;
         
        -- grant database privileges to the access role
        grant monitor, usage
            on database dev_plt_wrk_datascience_db
            to role dev_plt_wrk_datascience_db_ref_ar;
        -- grant schema privileges to the access role
        grant monitor, usage
            on all schemas in database dev_plt_wrk_datascience_db
            to role dev_plt_wrk_datascience_db_ref_ar;
        -- grant schema object privileges to the access role, repeat for each schema
        grant references
            on all tables in schema dev_plt_wrk_datascience_db.jsimth
            to role dev_plt_wrk_datascience_db_ref_ar;
        grant references
            on future tables in schema dev_plt_wrk_datascience_db.jsimth
            to role dev_plt_wrk_datascience_db_ref_ar;
        
        use role securityadmin;
        -- keep create statement minimal
        create role if not exists dev_plt_wrk_datascience_db_rw_ar comment = 'Local access role.';
        -- transfer ownership to local role
        grant ownership
            on role dev_plt_wrk_datascience_db_rw_ar 
            to role dev_plt_securityadmin_fr    
            revoke current grants;
        use role dev_plt_securityadmin_fr;   
        -- grant object privileges with local role
        use role dev_plt_sysadmin_fr;
        
        -- grant database privileges to the access role
        grant monitor, usage
                on database dev_plt_wrk_datascience_db
                to role dev_plt_wrk_datascience_db_rw_ar;

        -- grant schema privileges to the access role
        grant monitor, usage
                on all schemas in database dev_plt_wrk_datascience_db
                to role dev_plt_wrk_datascience_db_rw_ar;
 

        grant create table, create view, create file format, create stage, create sequence, create function, create procedure
                on all schemas in database dev_plt_wrk_datascience_db
                to role dev_plt_wrk_datascience_db_rw_ar;


        -- grant schema object privileges to the access role, repeat for each schema
                
        grant select, insert, update, delete, truncate
                on all tables in schema dev_plt_wrk_datascience_db.jjones
                to role dev_plt_wrk_datascience_db_rw_ar;

        grant select, insert, update, delete, truncate
                on future tables in schema dev_plt_wrk_datascience_db.jjones
                to role dev_plt_wrk_datascience_db_rw_ar;
                
        
        use role securityadmin;
        -- keep create statement minimal
        create role if not exists dev_plt_wrk_datascience_db_ro_ar comment = 'Local access role.';
        -- transfer ownership to local role
        grant ownership
            on role dev_plt_wrk_datascience_db_ro_ar 
            to role dev_plt_securityadmin_fr    
            revoke current grants;
        use role dev_plt_securityadmin_fr;   
        -- grant object privileges with local role
        use role dev_plt_sysadmin_fr;
         
        -- grant database privileges to the access role
        grant monitor, usage
            on database dev_plt_wrk_datascience_db
            to role dev_plt_wrk_datascience_db_ro_ar;
        -- grant schema privileges to the access role
        grant monitor, usage
            on all schemas in database dev_plt_wrk_datascience_db
            to role dev_plt_wrk_datascience_db_ro_ar;
        -- grant schema object privileges to the access role, repeat for each schema
       
        grant select
            on all tables in schema dev_plt_wrk_datascience_db.jjones
            to role dev_plt_wrk_datascience_db_ro_ar;
        grant select
            on future tables in schema dev_plt_wrk_datascience_db.jjones
            to role dev_plt_wrk_datascience_db_ro_ar;
        
        use role securityadmin;
        -- keep create statement minimal
        create role if not exists dev_plt_wrk_datascience_db_ref_ar comment = 'Local access role.';
        -- transfer ownership to local role
        grant ownership
            on role dev_plt_wrk_datascience_db_ref_ar 
            to role dev_plt_securityadmin_fr    
            revoke current grants;
        use role dev_plt_securityadmin_fr;   
        -- grant object privileges with local role
        use role dev_plt_sysadmin_fr;
         
        -- grant database privileges to the access role
        grant monitor, usage
            on database dev_plt_wrk_datascience_db
            to role dev_plt_wrk_datascience_db_ref_ar;
        -- grant schema privileges to the access role
        grant monitor, usage
            on all schemas in database dev_plt_wrk_datascience_db
            to role dev_plt_wrk_datascience_db_ref_ar;
        -- grant schema object privileges to the access role, repeat for each schema
        grant references
            on all tables in schema dev_plt_wrk_datascience_db.jjones
            to role dev_plt_wrk_datascience_db_ref_ar;
        grant references
            on future tables in schema dev_plt_wrk_datascience_db.jjones
            to role dev_plt_wrk_datascience_db_ref_ar;
        
        use role sysadmin; 
        create database if not exists dev_plt_dl_sap_db  comment='Raw layer source1';
        -- transfer ownership to local role
        grant ownership
            on database dev_plt_dl_sap_db
            to role dev_plt_sysadmin_fr
            revoke current grants
        ;
        grant ownership
            on all schemas in database dev_plt_dl_sap_db
            to role dev_plt_sysadmin_fr
            revoke current grants
        ;
        use role dev_plt_sysadmin_fr;
        -- drop public schema since it should never be used.
        drop schema dev_plt_dl_sap_db.public;

        -- creating schema 
        create schema if not exists dev_plt_dl_sap_db.sap   with managed access;
        
        Alter schema   dev_plt_dl_sap_db.sap Set comment='Schema for general use.'  ;
        
        -- access roles for database & schemas

        
        use role securityadmin;
        -- keep create statement minimal
        create role if not exists dev_plt_dl_sap_db_rw_ar comment = 'Local access role.';
        -- transfer ownership to local role
        grant ownership
            on role dev_plt_dl_sap_db_rw_ar 
            to role dev_plt_securityadmin_fr    
            revoke current grants;
        use role dev_plt_securityadmin_fr;   
        -- grant object privileges with local role
        use role dev_plt_sysadmin_fr;
        
        -- grant database privileges to the access role
        grant monitor, usage
                on database dev_plt_dl_sap_db
                to role dev_plt_dl_sap_db_rw_ar;

        -- grant schema privileges to the access role
        grant monitor, usage
                on all schemas in database dev_plt_dl_sap_db
                to role dev_plt_dl_sap_db_rw_ar;
 

        grant create table, create view, create file format, create stage, create sequence, create function, create procedure
                on all schemas in database dev_plt_dl_sap_db
                to role dev_plt_dl_sap_db_rw_ar;


        -- grant schema object privileges to the access role, repeat for each schema
                
        grant select, insert, update, delete, truncate
                on all tables in schema dev_plt_dl_sap_db.sap
                to role dev_plt_dl_sap_db_rw_ar;

        grant select, insert, update, delete, truncate
                on future tables in schema dev_plt_dl_sap_db.sap
                to role dev_plt_dl_sap_db_rw_ar;
                
        
        use role securityadmin;
        -- keep create statement minimal
        create role if not exists dev_plt_dl_sap_db_ro_ar comment = 'Local access role.';
        -- transfer ownership to local role
        grant ownership
            on role dev_plt_dl_sap_db_ro_ar 
            to role dev_plt_securityadmin_fr    
            revoke current grants;
        use role dev_plt_securityadmin_fr;   
        -- grant object privileges with local role
        use role dev_plt_sysadmin_fr;
         
        -- grant database privileges to the access role
        grant monitor, usage
            on database dev_plt_dl_sap_db
            to role dev_plt_dl_sap_db_ro_ar;
        -- grant schema privileges to the access role
        grant monitor, usage
            on all schemas in database dev_plt_dl_sap_db
            to role dev_plt_dl_sap_db_ro_ar;
        -- grant schema object privileges to the access role, repeat for each schema
       
        grant select
            on all tables in schema dev_plt_dl_sap_db.sap
            to role dev_plt_dl_sap_db_ro_ar;
        grant select
            on future tables in schema dev_plt_dl_sap_db.sap
            to role dev_plt_dl_sap_db_ro_ar;
        
        use role securityadmin;
        -- keep create statement minimal
        create role if not exists dev_plt_dl_sap_db_ref_ar comment = 'Local access role.';
        -- transfer ownership to local role
        grant ownership
            on role dev_plt_dl_sap_db_ref_ar 
            to role dev_plt_securityadmin_fr    
            revoke current grants;
        use role dev_plt_securityadmin_fr;   
        -- grant object privileges with local role
        use role dev_plt_sysadmin_fr;
         
        -- grant database privileges to the access role
        grant monitor, usage
            on database dev_plt_dl_sap_db
            to role dev_plt_dl_sap_db_ref_ar;
        -- grant schema privileges to the access role
        grant monitor, usage
            on all schemas in database dev_plt_dl_sap_db
            to role dev_plt_dl_sap_db_ref_ar;
        -- grant schema object privileges to the access role, repeat for each schema
        grant references
            on all tables in schema dev_plt_dl_sap_db.sap
            to role dev_plt_dl_sap_db_ref_ar;
        grant references
            on future tables in schema dev_plt_dl_sap_db.sap
            to role dev_plt_dl_sap_db_ref_ar;
        
        use role sysadmin; 
        create database if not exists dev_plt_il_db  comment='Integration database';
        -- transfer ownership to local role
        grant ownership
            on database dev_plt_il_db
            to role dev_plt_sysadmin_fr
            revoke current grants
        ;
        grant ownership
            on all schemas in database dev_plt_il_db
            to role dev_plt_sysadmin_fr
            revoke current grants
        ;
        use role dev_plt_sysadmin_fr;
        -- drop public schema since it should never be used.
        drop schema dev_plt_il_db.public;

        -- creating schema 
        create schema if not exists dev_plt_il_db.main   with managed access;
        
        Alter schema   dev_plt_il_db.main Set comment='Schema for general use.'  ;
        
        -- access roles for database & schemas

        
        use role securityadmin;
        -- keep create statement minimal
        create role if not exists dev_plt_il_db_rw_ar comment = 'Local access role.';
        -- transfer ownership to local role
        grant ownership
            on role dev_plt_il_db_rw_ar 
            to role dev_plt_securityadmin_fr    
            revoke current grants;
        use role dev_plt_securityadmin_fr;   
        -- grant object privileges with local role
        use role dev_plt_sysadmin_fr;
        
        -- grant database privileges to the access role
        grant monitor, usage
                on database dev_plt_il_db
                to role dev_plt_il_db_rw_ar;

        -- grant schema privileges to the access role
        grant monitor, usage
                on all schemas in database dev_plt_il_db
                to role dev_plt_il_db_rw_ar;
 

        grant create table, create view, create file format, create stage, create sequence, create function, create procedure
                on all schemas in database dev_plt_il_db
                to role dev_plt_il_db_rw_ar;


        -- grant schema object privileges to the access role, repeat for each schema
                
        grant select, insert, update, delete, truncate
                on all tables in schema dev_plt_il_db.main
                to role dev_plt_il_db_rw_ar;

        grant select, insert, update, delete, truncate
                on future tables in schema dev_plt_il_db.main
                to role dev_plt_il_db_rw_ar;
                
        
        use role securityadmin;
        -- keep create statement minimal
        create role if not exists dev_plt_il_db_ro_ar comment = 'Local access role.';
        -- transfer ownership to local role
        grant ownership
            on role dev_plt_il_db_ro_ar 
            to role dev_plt_securityadmin_fr    
            revoke current grants;
        use role dev_plt_securityadmin_fr;   
        -- grant object privileges with local role
        use role dev_plt_sysadmin_fr;
         
        -- grant database privileges to the access role
        grant monitor, usage
            on database dev_plt_il_db
            to role dev_plt_il_db_ro_ar;
        -- grant schema privileges to the access role
        grant monitor, usage
            on all schemas in database dev_plt_il_db
            to role dev_plt_il_db_ro_ar;
        -- grant schema object privileges to the access role, repeat for each schema
       
        grant select
            on all tables in schema dev_plt_il_db.main
            to role dev_plt_il_db_ro_ar;
        grant select
            on future tables in schema dev_plt_il_db.main
            to role dev_plt_il_db_ro_ar;
        
        use role securityadmin;
        -- keep create statement minimal
        create role if not exists dev_plt_il_db_ref_ar comment = 'Local access role.';
        -- transfer ownership to local role
        grant ownership
            on role dev_plt_il_db_ref_ar 
            to role dev_plt_securityadmin_fr    
            revoke current grants;
        use role dev_plt_securityadmin_fr;   
        -- grant object privileges with local role
        use role dev_plt_sysadmin_fr;
         
        -- grant database privileges to the access role
        grant monitor, usage
            on database dev_plt_il_db
            to role dev_plt_il_db_ref_ar;
        -- grant schema privileges to the access role
        grant monitor, usage
            on all schemas in database dev_plt_il_db
            to role dev_plt_il_db_ref_ar;
        -- grant schema object privileges to the access role, repeat for each schema
        grant references
            on all tables in schema dev_plt_il_db.main
            to role dev_plt_il_db_ref_ar;
        grant references
            on future tables in schema dev_plt_il_db.main
            to role dev_plt_il_db_ref_ar;
        
        use role sysadmin; 
        create database if not exists dev_plt_pl_db  comment='Presentation layer database';
        -- transfer ownership to local role
        grant ownership
            on database dev_plt_pl_db
            to role dev_plt_sysadmin_fr
            revoke current grants
        ;
        grant ownership
            on all schemas in database dev_plt_pl_db
            to role dev_plt_sysadmin_fr
            revoke current grants
        ;
        use role dev_plt_sysadmin_fr;
        -- drop public schema since it should never be used.
        drop schema dev_plt_pl_db.public;

        -- creating schema 
        create schema if not exists dev_plt_pl_db.main   with managed access;
        
        Alter schema   dev_plt_pl_db.main Set comment='Schema for general use.'  ;
        
        -- access roles for database & schemas

        
        use role securityadmin;
        -- keep create statement minimal
        create role if not exists dev_plt_pl_db_rw_ar comment = 'Local access role.';
        -- transfer ownership to local role
        grant ownership
            on role dev_plt_pl_db_rw_ar 
            to role dev_plt_securityadmin_fr    
            revoke current grants;
        use role dev_plt_securityadmin_fr;   
        -- grant object privileges with local role
        use role dev_plt_sysadmin_fr;
        
        -- grant database privileges to the access role
        grant monitor, usage
                on database dev_plt_pl_db
                to role dev_plt_pl_db_rw_ar;

        -- grant schema privileges to the access role
        grant monitor, usage
                on all schemas in database dev_plt_pl_db
                to role dev_plt_pl_db_rw_ar;
 

        grant create table, create view, create file format, create stage, create sequence, create function, create procedure
                on all schemas in database dev_plt_pl_db
                to role dev_plt_pl_db_rw_ar;


        -- grant schema object privileges to the access role, repeat for each schema
                
        grant select, insert, update, delete, truncate
                on all tables in schema dev_plt_pl_db.main
                to role dev_plt_pl_db_rw_ar;

        grant select, insert, update, delete, truncate
                on future tables in schema dev_plt_pl_db.main
                to role dev_plt_pl_db_rw_ar;
                
        
        use role securityadmin;
        -- keep create statement minimal
        create role if not exists dev_plt_pl_db_ro_ar comment = 'Local access role.';
        -- transfer ownership to local role
        grant ownership
            on role dev_plt_pl_db_ro_ar 
            to role dev_plt_securityadmin_fr    
            revoke current grants;
        use role dev_plt_securityadmin_fr;   
        -- grant object privileges with local role
        use role dev_plt_sysadmin_fr;
         
        -- grant database privileges to the access role
        grant monitor, usage
            on database dev_plt_pl_db
            to role dev_plt_pl_db_ro_ar;
        -- grant schema privileges to the access role
        grant monitor, usage
            on all schemas in database dev_plt_pl_db
            to role dev_plt_pl_db_ro_ar;
        -- grant schema object privileges to the access role, repeat for each schema
       
        grant select
            on all tables in schema dev_plt_pl_db.main
            to role dev_plt_pl_db_ro_ar;
        grant select
            on future tables in schema dev_plt_pl_db.main
            to role dev_plt_pl_db_ro_ar;
        
        use role securityadmin;
        -- keep create statement minimal
        create role if not exists dev_plt_pl_db_ref_ar comment = 'Local access role.';
        -- transfer ownership to local role
        grant ownership
            on role dev_plt_pl_db_ref_ar 
            to role dev_plt_securityadmin_fr    
            revoke current grants;
        use role dev_plt_securityadmin_fr;   
        -- grant object privileges with local role
        use role dev_plt_sysadmin_fr;
         
        -- grant database privileges to the access role
        grant monitor, usage
            on database dev_plt_pl_db
            to role dev_plt_pl_db_ref_ar;
        -- grant schema privileges to the access role
        grant monitor, usage
            on all schemas in database dev_plt_pl_db
            to role dev_plt_pl_db_ref_ar;
        -- grant schema object privileges to the access role, repeat for each schema
        grant references
            on all tables in schema dev_plt_pl_db.main
            to role dev_plt_pl_db_ref_ar;
        grant references
            on future tables in schema dev_plt_pl_db.main
            to role dev_plt_pl_db_ref_ar;
        
        use role sysadmin; 
        create database if not exists dev_plt_common_db  comment='Common database for udf, sprocs, file formats, stages, dev_mrxts_common_db';
        -- transfer ownership to local role
        grant ownership
            on database dev_plt_common_db
            to role dev_plt_sysadmin_fr
            revoke current grants
        ;
        grant ownership
            on all schemas in database dev_plt_common_db
            to role dev_plt_sysadmin_fr
            revoke current grants
        ;
        use role dev_plt_sysadmin_fr;
        -- drop public schema since it should never be used.
        drop schema dev_plt_common_db.public;

        -- creating schema 
        create schema if not exists dev_plt_common_db.util   with managed access;
        
        Alter schema   dev_plt_common_db.util Set comment='Schema for general use.'  ;
        
        -- access roles for database & schemas

        
        use role securityadmin;
        -- keep create statement minimal
        create role if not exists dev_plt_common_db_rw_ar comment = 'Local access role.';
        -- transfer ownership to local role
        grant ownership
            on role dev_plt_common_db_rw_ar 
            to role dev_plt_securityadmin_fr    
            revoke current grants;
        use role dev_plt_securityadmin_fr;   
        -- grant object privileges with local role
        use role dev_plt_sysadmin_fr;
        
        -- grant database privileges to the access role
        grant monitor, usage
                on database dev_plt_common_db
                to role dev_plt_common_db_rw_ar;

        -- grant schema privileges to the access role
        grant monitor, usage
                on all schemas in database dev_plt_common_db
                to role dev_plt_common_db_rw_ar;
 

        grant create table, create view, create file format, create stage, create sequence, create function, create procedure
                on all schemas in database dev_plt_common_db
                to role dev_plt_common_db_rw_ar;


        -- grant schema object privileges to the access role, repeat for each schema
                
        grant select, insert, update, delete, truncate
                on all tables in schema dev_plt_common_db.util
                to role dev_plt_common_db_rw_ar;

        grant select, insert, update, delete, truncate
                on future tables in schema dev_plt_common_db.util
                to role dev_plt_common_db_rw_ar;
                
        
        use role securityadmin;
        -- keep create statement minimal
        create role if not exists dev_plt_common_db_ro_ar comment = 'Local access role.';
        -- transfer ownership to local role
        grant ownership
            on role dev_plt_common_db_ro_ar 
            to role dev_plt_securityadmin_fr    
            revoke current grants;
        use role dev_plt_securityadmin_fr;   
        -- grant object privileges with local role
        use role dev_plt_sysadmin_fr;
         
        -- grant database privileges to the access role
        grant monitor, usage
            on database dev_plt_common_db
            to role dev_plt_common_db_ro_ar;
        -- grant schema privileges to the access role
        grant monitor, usage
            on all schemas in database dev_plt_common_db
            to role dev_plt_common_db_ro_ar;
        -- grant schema object privileges to the access role, repeat for each schema
       
        grant select
            on all tables in schema dev_plt_common_db.util
            to role dev_plt_common_db_ro_ar;
        grant select
            on future tables in schema dev_plt_common_db.util
            to role dev_plt_common_db_ro_ar;
        
        use role securityadmin;
        -- keep create statement minimal
        create role if not exists dev_plt_common_db_ref_ar comment = 'Local access role.';
        -- transfer ownership to local role
        grant ownership
            on role dev_plt_common_db_ref_ar 
            to role dev_plt_securityadmin_fr    
            revoke current grants;
        use role dev_plt_securityadmin_fr;   
        -- grant object privileges with local role
        use role dev_plt_sysadmin_fr;
         
        -- grant database privileges to the access role
        grant monitor, usage
            on database dev_plt_common_db
            to role dev_plt_common_db_ref_ar;
        -- grant schema privileges to the access role
        grant monitor, usage
            on all schemas in database dev_plt_common_db
            to role dev_plt_common_db_ref_ar;
        -- grant schema object privileges to the access role, repeat for each schema
        grant references
            on all tables in schema dev_plt_common_db.util
            to role dev_plt_common_db_ref_ar;
        grant references
            on future tables in schema dev_plt_common_db.util
            to role dev_plt_common_db_ref_ar;
        
        use role sysadmin; 
        create database if not exists dev_plt_wrk_analyst_db  comment='Workspace database for the analyst team';
        -- transfer ownership to local role
        grant ownership
            on database dev_plt_wrk_analyst_db
            to role dev_plt_sysadmin_fr
            revoke current grants
        ;
        grant ownership
            on all schemas in database dev_plt_wrk_analyst_db
            to role dev_plt_sysadmin_fr
            revoke current grants
        ;
        use role dev_plt_sysadmin_fr;
        -- drop public schema since it should never be used.
        drop schema dev_plt_wrk_analyst_db.public;

        -- creating schema 
        create schema if not exists dev_plt_wrk_analyst_db.main   with managed access;
        create schema if not exists dev_plt_wrk_analyst_db.jsimth   with managed access;
        
        Alter schema   dev_plt_wrk_analyst_db.main Set comment='Schema for general use.'  ;
        Alter schema   dev_plt_wrk_analyst_db.jsimth Set comment='Schema for general use.'  ;
        
        -- access roles for database & schemas

        
        use role securityadmin;
        -- keep create statement minimal
        create role if not exists dev_plt_wrk_analyst_db_rw_ar comment = 'Local access role.';
        -- transfer ownership to local role
        grant ownership
            on role dev_plt_wrk_analyst_db_rw_ar 
            to role dev_plt_securityadmin_fr    
            revoke current grants;
        use role dev_plt_securityadmin_fr;   
        -- grant object privileges with local role
        use role dev_plt_sysadmin_fr;
        
        -- grant database privileges to the access role
        grant monitor, usage
                on database dev_plt_wrk_analyst_db
                to role dev_plt_wrk_analyst_db_rw_ar;

        -- grant schema privileges to the access role
        grant monitor, usage
                on all schemas in database dev_plt_wrk_analyst_db
                to role dev_plt_wrk_analyst_db_rw_ar;
 

        grant create table, create view, create file format, create stage, create sequence, create function, create procedure
                on all schemas in database dev_plt_wrk_analyst_db
                to role dev_plt_wrk_analyst_db_rw_ar;


        -- grant schema object privileges to the access role, repeat for each schema
                
        grant select, insert, update, delete, truncate
                on all tables in schema dev_plt_wrk_analyst_db.main
                to role dev_plt_wrk_analyst_db_rw_ar;

        grant select, insert, update, delete, truncate
                on future tables in schema dev_plt_wrk_analyst_db.main
                to role dev_plt_wrk_analyst_db_rw_ar;
                
        
        use role securityadmin;
        -- keep create statement minimal
        create role if not exists dev_plt_wrk_analyst_db_ro_ar comment = 'Local access role.';
        -- transfer ownership to local role
        grant ownership
            on role dev_plt_wrk_analyst_db_ro_ar 
            to role dev_plt_securityadmin_fr    
            revoke current grants;
        use role dev_plt_securityadmin_fr;   
        -- grant object privileges with local role
        use role dev_plt_sysadmin_fr;
         
        -- grant database privileges to the access role
        grant monitor, usage
            on database dev_plt_wrk_analyst_db
            to role dev_plt_wrk_analyst_db_ro_ar;
        -- grant schema privileges to the access role
        grant monitor, usage
            on all schemas in database dev_plt_wrk_analyst_db
            to role dev_plt_wrk_analyst_db_ro_ar;
        -- grant schema object privileges to the access role, repeat for each schema
       
        grant select
            on all tables in schema dev_plt_wrk_analyst_db.main
            to role dev_plt_wrk_analyst_db_ro_ar;
        grant select
            on future tables in schema dev_plt_wrk_analyst_db.main
            to role dev_plt_wrk_analyst_db_ro_ar;
        
        use role securityadmin;
        -- keep create statement minimal
        create role if not exists dev_plt_wrk_analyst_db_ref_ar comment = 'Local access role.';
        -- transfer ownership to local role
        grant ownership
            on role dev_plt_wrk_analyst_db_ref_ar 
            to role dev_plt_securityadmin_fr    
            revoke current grants;
        use role dev_plt_securityadmin_fr;   
        -- grant object privileges with local role
        use role dev_plt_sysadmin_fr;
         
        -- grant database privileges to the access role
        grant monitor, usage
            on database dev_plt_wrk_analyst_db
            to role dev_plt_wrk_analyst_db_ref_ar;
        -- grant schema privileges to the access role
        grant monitor, usage
            on all schemas in database dev_plt_wrk_analyst_db
            to role dev_plt_wrk_analyst_db_ref_ar;
        -- grant schema object privileges to the access role, repeat for each schema
        grant references
            on all tables in schema dev_plt_wrk_analyst_db.main
            to role dev_plt_wrk_analyst_db_ref_ar;
        grant references
            on future tables in schema dev_plt_wrk_analyst_db.main
            to role dev_plt_wrk_analyst_db_ref_ar;
        
        use role securityadmin;
        -- keep create statement minimal
        create role if not exists dev_plt_wrk_analyst_db_rw_ar comment = 'Local access role.';
        -- transfer ownership to local role
        grant ownership
            on role dev_plt_wrk_analyst_db_rw_ar 
            to role dev_plt_securityadmin_fr    
            revoke current grants;
        use role dev_plt_securityadmin_fr;   
        -- grant object privileges with local role
        use role dev_plt_sysadmin_fr;
        
        -- grant database privileges to the access role
        grant monitor, usage
                on database dev_plt_wrk_analyst_db
                to role dev_plt_wrk_analyst_db_rw_ar;

        -- grant schema privileges to the access role
        grant monitor, usage
                on all schemas in database dev_plt_wrk_analyst_db
                to role dev_plt_wrk_analyst_db_rw_ar;
 

        grant create table, create view, create file format, create stage, create sequence, create function, create procedure
                on all schemas in database dev_plt_wrk_analyst_db
                to role dev_plt_wrk_analyst_db_rw_ar;


        -- grant schema object privileges to the access role, repeat for each schema
                
        grant select, insert, update, delete, truncate
                on all tables in schema dev_plt_wrk_analyst_db.jsimth
                to role dev_plt_wrk_analyst_db_rw_ar;

        grant select, insert, update, delete, truncate
                on future tables in schema dev_plt_wrk_analyst_db.jsimth
                to role dev_plt_wrk_analyst_db_rw_ar;
                
        
        use role securityadmin;
        -- keep create statement minimal
        create role if not exists dev_plt_wrk_analyst_db_ro_ar comment = 'Local access role.';
        -- transfer ownership to local role
        grant ownership
            on role dev_plt_wrk_analyst_db_ro_ar 
            to role dev_plt_securityadmin_fr    
            revoke current grants;
        use role dev_plt_securityadmin_fr;   
        -- grant object privileges with local role
        use role dev_plt_sysadmin_fr;
         
        -- grant database privileges to the access role
        grant monitor, usage
            on database dev_plt_wrk_analyst_db
            to role dev_plt_wrk_analyst_db_ro_ar;
        -- grant schema privileges to the access role
        grant monitor, usage
            on all schemas in database dev_plt_wrk_analyst_db
            to role dev_plt_wrk_analyst_db_ro_ar;
        -- grant schema object privileges to the access role, repeat for each schema
       
        grant select
            on all tables in schema dev_plt_wrk_analyst_db.jsimth
            to role dev_plt_wrk_analyst_db_ro_ar;
        grant select
            on future tables in schema dev_plt_wrk_analyst_db.jsimth
            to role dev_plt_wrk_analyst_db_ro_ar;
        
        use role securityadmin;
        -- keep create statement minimal
        create role if not exists dev_plt_wrk_analyst_db_ref_ar comment = 'Local access role.';
        -- transfer ownership to local role
        grant ownership
            on role dev_plt_wrk_analyst_db_ref_ar 
            to role dev_plt_securityadmin_fr    
            revoke current grants;
        use role dev_plt_securityadmin_fr;   
        -- grant object privileges with local role
        use role dev_plt_sysadmin_fr;
         
        -- grant database privileges to the access role
        grant monitor, usage
            on database dev_plt_wrk_analyst_db
            to role dev_plt_wrk_analyst_db_ref_ar;
        -- grant schema privileges to the access role
        grant monitor, usage
            on all schemas in database dev_plt_wrk_analyst_db
            to role dev_plt_wrk_analyst_db_ref_ar;
        -- grant schema object privileges to the access role, repeat for each schema
        grant references
            on all tables in schema dev_plt_wrk_analyst_db.jsimth
            to role dev_plt_wrk_analyst_db_ref_ar;
        grant references
            on future tables in schema dev_plt_wrk_analyst_db.jsimth
            to role dev_plt_wrk_analyst_db_ref_ar;
        