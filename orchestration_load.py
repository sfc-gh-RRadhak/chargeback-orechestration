import json
import logging
import math
import sys
import os
import traceback
import argparse
from concurrent import futures
import datetime
import snowflake.connector
from snowflake_db import SnowflakeDB
from datetime import datetime,timedelta
import itertools as it
from operator import itemgetter
import traceback
import json
import logging
import yaml
file_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(file_path + "/..")
LOG_FORMAT = (
    "%(asctime)s.%(msecs)d [%(levelname)s] [%(filename)s:%(lineno)d] %(message)s"
)
LOG_DATEFMT = "%Y-%m-%d:%H:%M:%S"

"""
Orchestration_Load : Implements the following to ingest and process the files to the final presentation layer. It executes the acquisition and data process scripts based on the configuration  supplied in the json files under the /config folder

Accquistion Load :  This load collects all the metrics from snowflake to text files.

Dataload Loads :  This is orchestrated after the acquisition load.  It takes data from stage folders and transforms to final presentation tables

load_config:  load configuration files contains the load SQL that needs to be executed. All the config files are in config/chargeback and config/monitoring 
example :       
{
    
    "dimension":[
        {     
            "operation": [
                { 
                    "step":1, 
                    "load_type": "execute",
                    "load_name":"cb_resource_dm_ld",
                    "load_sql":"sql/chargeback/400_dimension/cb_resource_dm_ld.sql" 
                } ,
                { 
                    "step":2, 
                    "load_type": "execute",
                    "load_name":"cb_service_type_dm_ld",
                    "load_sql":"sql/chargeback/400_dimension/cb_service_type_dm_ld.sql"
                } ,
                { 
                    "step":3, 
                    "load_type": "execute",
                    "load_name":"date_dm_ld",
                    "load_sql":"sql/chargeback/400_dimension/date_dm_ld.sql" 
                } 


            ]
        }  


    ]

}

parameters dictionary variable:  parameter variable is constructed in the program to substitute values in the congfig files {variable}
{'minutes': 20, 'lastCtrlDt': '2020-07-24 13:18:25', 
'config': '/Users/rradhakrishnam/Desktop/code/mck_env/code/dna-snowflake/config/monitoring/acquisition.json', 
'dbconfig': '/Users/rradhakrishnam/Desktop/code/mck_env/code/dna-snowflake/config/monitoring/config.json', 
'source_db_config': '/Users/rradhakrishnam/Desktop/code/mck_env/code/dna-snowflake/config/monitoring/source_config.json', 
'load': 'acquisition', 
'file_path': '/Users/rradhakrishnam/Desktop/code/mck_env/code/dna-snowflake', 
'stage': 'dev_plt_common_db.util.account_usage_stg', 
'environment': 'dev', 
'load_type': 'copy_into', 
'load_name': 'query_history_acq', 
'load_sql': 'sql/monitoring/100_acquisition/query_history_acq.sql', 
'timestampStr': '07242020133829_536952', 
'fileName': 'query_history_acq_07242020133829_536952.csv'}

"""


class Orchestration_Load():
    def __init__( self,log=None):
        self.log=log

    " read the configuration files and stores them to  a variable"
    def read_config(self, config_file):
        """Read JSON config file, parses it"""
        self.log.info(f'config_file-->{config_file}')
        with open(config_file) as json_config_file:
            config = json.load(json_config_file)
        return config

    

    def get_sf_connection_list(self, config_file):
        config=self.read_config(config_file)
        connection_list = [items for items in config['snowflake_connection']]
        return connection_list


    
    """ This is called from the main () to start the load process
    - source_db_config variable is only supplied for the acquisition load.  For the acquisition load the source connections are looped through.

    - for each database connection call the data_load_snowflake
    
    """         
    def execute_snowflake_usage_load(self,parameter):
        try:            
            if 'source_db_config' in parameter: 
                # 
                connection_list=self.get_sf_connection_list(parameter['source_db_config'])
                self.log.info("entering source loads") 
                self.log.info("connection_list-->{connection_list}") 
                # connections are looped through from the source_db_config 
                for connection in connection_list:
                    parameter['stage']=connection['stage']
                    dbconn=SnowflakeDB(config=connection)
                    self.data_load_snowflake(dbconn,parameter)
            else:
                # Data is loaded from the raw to the presentation layer 
                dbconn=SnowflakeDB(config=parameter)
                self.log.info(f"parameter---> {parameter}") 
                self.log.info("entering reqular load") 
                self.data_load_snowflake(dbconn,parameter)

        except Exception as e:
            self.log.error(f"error in execute_snowflake_usage_load :  Error Code {str(e)} ")
            self.log.error(f"{e}\n{traceback.format_exc()}")
            raise 

    def load_execution(self,dbconn,parameter):
        try:
            stored_proc_list=['storedproc','storedprocedure']
            file_name=get_file_name(parameter)
            parameter['fileName']=file_name
            load_sql_file_path=f"{parameter['file_path']}/{parameter['load_sql']}"
            self.log.info("load_sql_file_path-->{load_sql_file_path}")
            print("parameter-->",parameter)
            sql=''
            is_procedure=0 # looking for the procedure keyword

            with open( load_sql_file_path,"r") as f:
                if parameter['load_type'].lower() in stored_proc_list:
                    lines = f.readlines() 
                    for line in lines:
                         
                        if str('create or replace procedure').strip().upper()  in line.strip().upper():
                            is_procedure=1  # first time it found the create or replace procedure
                            sql=sql+line.format(**parameter)
                        if is_procedure==0: #Reqular lines before the create or replace procedure
                            sql=sql+line .format(**parameter)
                        elif is_procedure==1 and str('create or replace procedure').strip().upper() not in line.strip().upper(): # now processing the rest of the lines
                            sql=sql+line
                else:
                    sql=str(f.read()).format(**parameter)
             
            self.log.info("********parameter********>")
            self.log.info(f'parameter-->{parameter}')
            self.log.info(sql)
            self.log.info("****************")
            
            if parameter['load_type'].lower()=='execute':
                sql_list=sql.split(';')
                for sql in sql_list:
                    dbconn.execute(sql)
            elif parameter['load_type'].lower() in stored_proc_list:
                print("parameter['load_type']-->",parameter['load_type'].lower()) 
                dbconn.execute_stream(sql) 
            else:
                 self.log.info(f"""{parameter['load_type']}  is not present in the yaml""")

        except Exception as e:
            self.log.error(f"error in load_execution :  Error Code {str(e)} ")
            self.log.error(f"{e}\n{traceback.format_exc()}")
            raise 

    """ JSON load sequence format is loaded and sorted into a list in the order of the execution sequence."""
    def data_load_snowflake(self,dbconn,parameter):
        try:
            ll=[]
            #config=self.read_config(parameter['config'])
            #self.log.info(f'config --> {config}') 
            load=parameter['load']
            with open(parameter['config']) as file:
                object_list=yaml.load(file,Loader=yaml.FullLoader)

            operation_list= object_list[load]['operation']
            print(operation_list)
            copy_list = sorted(operation_list, key=itemgetter('step'))
            print("copy_list",copy_list)
            for key, value in it.groupby(copy_list, key=itemgetter('step')):
                    l=[]
                    for i in value:
                        s={}
                        s['load_type']=i['load_type']
                        s['load_name']=i['load_name']
                        s['load_sql']=i['load_sql']
                        l.append(s)
                    ll.append(l)
            #print ('ll',ll)
             
            for item in ll:
                for i in item:
                    parameter['load_type']=i['load_type']
                    parameter['load_name']=i['load_name']
                    parameter['load_sql']=i['load_sql']
                    self.load_execution(dbconn,parameter)
 
        except Exception as e:
            self.log.error(f"error in snowflake_acquistion_load :  Error Code {str(e)} ")
            self.log.error(f"{e}\n{traceback.format_exc()}")
            raise 

""" Constructs the file for the given load name"""
def get_file_name(parameters):
    now = datetime.now()
    timestampStr = now.strftime("%m%d%Y%H%M%S_%f")
    parameters['timestampStr']=timestampStr
    file_name="{load_name}_{timestampStr}.csv".format(**parameters)
    return file_name
 
"""  Parameters  passed to the  orchestration program
load: following load name is passed to orchestrate the load program.    'acquisition' ,'raw,'integration','derivation' ,'dimension','fact' 
dbconfig: contains the configuration of the destination sf database or a/c

following are optional parameters 
--minutes or --days : this vaule is converted to datetime and passed on to the loader.
--load_config:  contains the scripts that needs to run based on the load name. 
--source_db_config: contains all the source sf accounts 
--logfilepath: location of the application log path :/Users/sam.andersonmckesson.com/code/log/
 
"""
def parse_args():
    """Parses the arguments and returns them"""
    parser = argparse.ArgumentParser(description='Run the acquisition load')
    parser.add_argument('load', type=str, action='store',   help='load name ' )
    parser.add_argument('dbconfig', type=str, action='store',   help='dbconfig contains the SF connection to the main a/c ')
   
    parser.add_argument('--minutes', type=str, action='store', dest='minutes', help='minutes positive number. Default is 15 minutes', default='NONE')  
    parser.add_argument('--days', type=str, action='store', dest='days', help='minutes positive number. Default is 15 minutes', default='NONE')  
    parser.add_argument('--load_config', type=str, action='store', dest='load_config', help='load config ', default='config/acquisition.json')
    parser.add_argument('--source_db_config', type=str, action='store', dest='source_db_config', help='load config ', default='NONE')
    parser.add_argument('--logfilepath', type=str, action='store', dest='logfilepath', help='log file path name ', default='/Users/rradhakrishnam/Desktop/code/log')
    parser.add_argument('--stage_file_path_pattern', type=str, action='store', dest='stage_file_path_pattern', help=' file path in the SF stage', default='YYYY/MM/DD')
   
    
    return parser.parse_args()


"""
Chargeback:
python3 orchestration_load.py acquisition config/chargeback/config.json --minutes 20 --load_config config/chargeback/acquisition.yaml --source_db_config config/chargeback/source_config.json
python3 orchestration_load.py raw config/chargeback/config.json --load_config 'config/chargeback/raw.yaml'                           
python3 orchestration_load.py integration config/chargeback/config.json --load_config 'config/chargeback/integration.yaml'
python3 orchestration_load.py derivation config/chargeback/config.json --load_config 'config/chargeback/derivation.yaml'
python3 orchestration_load.py dimension config/chargeback/config.json --load_config 'config/chargeback/dimension.yaml'
python3 orchestration_load.py fact config/chargeback/config.json --load_config 'config/chargeback/fact.yaml'

Monitoring :
python3 orchestration_load.py acquisition config/monitoring/config.json --minutes 20 --load_config config/monitoring/acquisition.json --source_db_config config/monitoring/source_config.json --logfilepath /Users/rradhakrishnam/Desktop/code/log/

"""

def main():

    args = parse_args()
    (len(vars(args)))
     
    if len(vars(args)) > 0 :
        minutes = args.minutes
        days=args.days
        load =args.load
        source_db_config=args.source_db_config
        logfilepath=args.logfilepath
        dbconfig=args.dbconfig
        

        load_config=f'{file_path}/{args.load_config}' 

        if minutes=='NONE' and days=='NONE':
            minutes=15
        if minutes!='NONE' :
            minutes=abs(int(minutes))
        if days!='NONE':
            minutes=abs(int(days))*24*60
        
        print ("days-->",days)
        print ("minutes-->",minutes)
         
        dbconfig=f'{file_path}/{dbconfig}' 
        now = datetime.now()- timedelta(minutes=minutes)

        log_file_name=f'{logfilepath}{load}_{now.strftime("%m%d%Y")}.log'
        logging.basicConfig(filename=f'{log_file_name}',level=logging.DEBUG, format=LOG_FORMAT, datefmt=LOG_DATEFMT)    
        logging.info("log_file_name-->{log_file_name}")
        logging.info("sys.path: {sys.path}")
        logging.info("file_path: {file_path}")
        lastCtrlDt = now.strftime("%Y-%m-%d %H:%M:%S")
        
        logging.info(f'Control datetime {lastCtrlDt} --- minutes: {minutes} ')
        logging.info (f'load --> {load}')
        logging.info (f'load_config --> {load_config}' )
        logging.info (f'source_db_config --> {source_db_config}')
        logging.info (f'dbconfig --> {dbconfig}')
        
        parameter={}
        parameter['minutes']=minutes
        parameter['lastCtrlDt']=lastCtrlDt
        parameter['config']= load_config
        parameter['dbconfig']= dbconfig

        
        print("source_db_config",source_db_config)
        if source_db_config !='NONE':
            source_db_config=f'{file_path}/{source_db_config}' 
            parameter['source_db_config']=source_db_config 
         
        parameter['load']=load
        parameter['file_path']=file_path
        logging.info (f'parameter --> {parameter}')
        print (parameter)
        ac=Orchestration_Load(logging)
        connection=ac.read_config(dbconfig)
        for key,value in connection.items():
            parameter[key]=value
        parameter['stage_file_path_pattern']=args.stage_file_path_pattern
        
        print(parameter)
        ac.execute_snowflake_usage_load(parameter)
if __name__ == '__main__': main()
