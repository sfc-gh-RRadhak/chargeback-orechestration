# dna-snowflake

orchestration_load.py
orchestrator.py
snowflake_db.py

Folder structure :

    Load Configuration:
        config/chargeback
        config/monitoring 
    
    Load Configuration Setup:
        {
            "{process load name}":[
                {
                    "operation": [
                        { 
                            "step":1, 
                            "load_type": "execute" or "copy into"
                            "load_name":"{load name}",
                            "load_sql":"sql/chargeback/410_fact/{load name}.sql"   
                        }  ,
                        { 
                            "step":2, 
                            "load_type": "execute" or "copy into"
                            "load_name":"{load_name}",
                            "load_sql":"sql/chargeback/410_fact/{load_name}.sql"   
                        } 
                    ]
                }  
            ]
        }

        -  process load name should be replaced  in the {process load name}. 
            "{process load name}" should contain one of the following name acquisition,raw,integration,derivation,dimension,fact
        -  "load_name" is  name of the load SQL file and it is used to construct the file name.
           "load_name" name should be unique 
        -  "step" contains the execution sequence number. In the python code "step" tag is used 
            group the execution steps for the execution.
        -  "Operation" tag is a yaml list object that contains list of SQL execution files.
        -  "load_sql"  contains the location of the SQL file. multiple SQL statements  are separated by 
            ";" is used to separate multiple statements in a single file.

    SQL template:
        Chargeback
            sql/chargeback/100_acquisition
            sql/chargeback/200_raw
        monitoring 
            sql/monitoring/100_acquisition
        
        
        chargeback setup
            sql/chargeback/setup
        monitoring set up
            sql/monitoring/setup
    


 
example:

chargeback:
    python3 orchestration_load.py acquisition config/chargeback/config.json --minutes 20 --load_config config/chargeback/acquisition.yaml --source_db_config config/chargeback/source_config.json
    python3 orchestration_load.py raw config/chargeback/config.json --load_config 'config/chargeback/raw.yaml'
    

monitoring:
 
    python3 orchestration_load.py acquisition config/monitoring/config.json --minutes 20 --load_config config/monitoring/acquisition.yaml --source_db_config config/monitoring/source_config.yaml --logfilepath /Users/rradhakrishnam/Desktop/code/log/

    


orchestrator.py

orchestrator.py python code is used to execute the sequence of load scripts defined in the #####_orchestrator.yaml. In the "script" tag include the python calling parameters

python3 orchestrator.py config/chargeback/chargeback_orchestrator.yaml

python3 orchestrator.py config/monitoring/monitoring_orchestrator.yaml