import json
import datetime
import snowflake.connector
import base64
import pyodbc
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import dsa
from cryptography.hazmat.primitives import serialization

class SnowflakeDB():
    """Class to encapsulate connection to   Snowflake accounts"""

    def __init__(self, config=None,config_file=None):
        """Reads configuration file and initializes object"""
        if config_file is None:
            print ("config-->",config_file)
            self.config=config
            self._debug = (self.config['debug'].lower() == 'true')
        else:
            self._read_config(config_file=config_file)
         
        connection_method=int(self.config['connection_method'])
        if connection_method==1 :
            self.connect_to_snowflake_ValuePair()
        else:
            self.connect_to_snowflake()
            self.field_names_by_type = {}
        print(f"""self.config --> {self.config}""")
        print(f"""database-->> {self.config['database']}""")
        print(f"""schema-->> {self.config['schema']}""")
        print(f"""warehouse-->> {self.config['warehouse']}""")
        print(f"""role-->> {self.config['role']}""")

        self.establish_env(self.config['database'], self.config['schema'], self.config['warehouse'],self.config['role'])
        
    def connect_to_snowflake_ValuePair(self):
        print("PrivateKeyLocation",self.config["PrivateKeyLocation"])
         
        with open(self.config["PrivateKeyLocation"], "rb") as key:
            p_key = serialization.load_pem_private_key(
                    key.read(),
                    password=self.config["PrivateKeyPhrase"].encode(),
                    backend=default_backend()
            )
            pkb = p_key.private_bytes(
                    encoding=serialization.Encoding.DER,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption())
        self.con = snowflake.connector.connect(
                account=self.config['account'],
                user=self.config['username'],
                private_key=pkb)
        self.cur = self.con.cursor()

    def connect_to_snowflake(self):
        self.con =snowflake.connector.connect (
            user=self.config['username'],
            password=self.config['password'],
            account=self.config['account']
        )
        self.cur = self.con.cursor()
    
    def establish_env(self, database, schema, warehouse=None,role=None):
        if role:
            print("USE ROLE {}".format(role))
            self.execute("USE ROLE {}".format(role))
        if database:
            print("USE DATABASE {}".format(database))
            self.execute("USE DATABASE {}".format(database))

        if schema:
            print("USE SCHEMA {}".format(schema))
            self.execute("USE SCHEMA {}".format(schema))

        if warehouse:
            print("USE WAREHOUSE {}".format(warehouse))
            self.execute("USE WAREHOUSE {}".format(warehouse))
            
    def validate(self):
        self.cur.execute("SELECT current_version()") 
        one_row = self.cur.fetchone()
        print(one_row[0])

    def _read_config(self,config=None, config_file=None):
        """Read JSON config file, parses it"""
        if config_file is not None:
            with open(config_file) as json_config_file:
                self.config = json.load(json_config_file)
        else:
            self.config=config
        self._debug = (self.config['debug'].lower() == 'true')
            
    def _connect(self, connection_string):
        """connect to database incl. some initialization via USE command"""
        self.connection_string = connection_string.encode('utf-8')
        try:
            self.log_message('Connecting to ' + connection_string + '...')

            self.con = pyodbc.connect('DSN=' + connection_string,autocommit=True)
            self.con.setencoding('utf-8')

            self.cur = self.con.cursor()

            self.execute("USE ROLE {}".format(self.get_defalut_role()))

            self.execute("USE WAREHOUSE " + self.config['warehouse'])
            self.execute("USE DATABASE " + self.config['database'])
            self.execute("USE SCHEMA " + self.config['schema'])
 
            self.connection_details = self.connection_information()
            self.log_message('Connected to ' + str(self.connection_details))
        except:
            self.connection_details = {'account_name': '', 'region': '', 'database': '', 'primary': '',
                                       'replication_group': ''}
      
            raise

    def get_etl_schema(self):
        return self.config['etl_schema']

    def get_etl_database(self):
        return self.config['etl_database']

    def get_etl_role(self):
        return self.config['etl_role']

    def get_schema(self):
        return self.config['schema']

    def get_database(self):
        return self.config['database']

    def set_environment(self,database=None,schema=None,warehouse=None):
        if warehouse:
            self.execute("USE WAREHOUSE " + warehouse)
        else:
            self.execute("USE WAREHOUSE " + self.config['warehouse'])

        if database:
            self.execute("USE DATABASE " + database)
        else:
            self.execute("USE DATABASE " + self.config['database'])
        if schema:
            self.execute("USE SCHEMA " + schema)
        else:
            self.execute("USE SCHEMA " + self.config['schema'])


    def set_etl_environment(self,database=None,schema=None,warehouse=None):
        self.execute("USE ROLE {}".format(self.config["etl_role"]))
        if warehouse:
            self.execute("USE WAREHOUSE " + warehouse)
        else:
            self.execute("USE WAREHOUSE " + self.config['warehouse'])

        if database:
            self.execute("USE DATABASE " + database)
        else:
            self.execute("USE DATABASE " + self.config['etl_database'])
        if schema:
            self.execute("USE SCHEMA " + schema)
        else:
            self.execute("USE SCHEMA " + self.config['etl_schema'])

    def get_defalut_role(self):
        if self._replication:
            return self.config['admin_role']
        else:
            return self.config['etl_role']

    def log_message(self, s):
        if self._debug:
            print (str(datetime.datetime.now()) + ' - ' + s)

 
     
    def current_connection(self):
        return self.connection_string
 

    def connect_to_other_db(self):
        """Switches to the other db connection within this pair of connection"""
        other_db = self.other_connection()
        self._connect(other_db)

    def execute(self, sql):
        self.log_message(sql)
        self.cur.execute(sql)
    
    def execute_stream(self, sql):
        self.log_message(sql)
        self.con.execute_stream(sql)
    

    def fetchall(self, sql):
        self.log_message(sql)
        return self.cur.execute(sql).fetchall()

    def execute_dml(self,sql):

        """Execute a INSERT command and return the output ( rows affected)  as list of dictionary elements  """
        self.log_message(sql)
        self.cur.execute(sql)
        dml_output = self.cur.execute('SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))').fetchall()
        result=[
                {'number of rows affected':row[0]} \
                for row in dml_output]

        return result
    def copy_to_file(self, sql):
        """Executes a COPY command and will return the output (file, status, rows_parses, rows_loaded, errors_seen etc.)
        as list of dictionary elements """
        self.log_message(sql)
        self.cur.execute(sql)
        copy_output = self.cur.execute('SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))').fetchall()
        result = []
        print ("copy_output",copy_output)
        
        result = [
                {'unloaded': row[0], 'input_bytes': row[1], 'output_bytes': row[2] }\
                for row in copy_output if '0 files processed' not in str(row[0]) ]
        return result
    def copy(self, sql):
        """Executes a COPY command and will return the output (file, status, rows_parses, rows_loaded, errors_seen etc.)
        as list of dictionary elements """
        self.log_message(sql)
        self.cur.execute(sql)
        copy_output = self.cur.execute('SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))').fetchall()

        result = [
            {'file': row[0], 'status': row[1], 'rows_parsed': row[2], 'rows_loaded': row[3], 'error_limit': row[4],
             'errors_seen': row[5], \
             'first_error': row[6], 'first_error_line': row[7], 'first_error_character': row[8],
             'first_error_column_name': row[9]} \
            for row in copy_output]

        return result

    def tables(self):
        """Returns all tables in the current db and schema. Result is a list of dictionary items,
        incl. fields table_name, database_name, schema_name"""
        self.cur.execute('SHOW TABLES')
        rows = self.cur.execute('SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))').fetchall()
        return [{'table_name': row[1], 'database_name': row[2], 'schema_name': row[3]} for row in rows]

    def row_count(self, database_name, schema_name, table_name):
        fully_qualified_table = database_name + '.' + schema_name + '.' + table_name
        return int(self.cur.execute('SELECT COUNT(*) FROM ' + fully_qualified_table).fetchall()[0][0])

    def field_count(self, database_name, schema_name, table_name):
        return len(self.cur.execute(
            "SELECT * FROM information_schema.columns WHERE table_catalog = '" + database_name.upper() + \
            "' AND table_schema = '" + schema_name.upper() + \
            "' AND table_name = '" + table_name.upper() + "'").fetchall())

    def refresh(self):
        """Executes a DATABASE REFRESH, which triggers the replication. Will only work, if we are connected
        to the secondary database"""
        if self.connection_details['primary'] == 'false':
            self.execute('ALTER DATABASE ' + self.config['database'] + ' REFRESH')
        else:
            self.log_message('Warning: Database is primary, cannot refresh.')

    def stage_properties(self, stage_name):
        """Retrieves information about the stage and returns it as dictionary, incl. the keys name and url"""
        self.cur.execute("SHOW STAGES like '" + stage_name + "'")
        properties = self.cur.execute("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))").fetchone()
        return {"name": properties[1], "url": properties[4]}

     

def main():
    dbconn=SnowflakeDB(config_file='/Users/rradhakrishnam/Documents/Project/chargeback-orechestration/config/map_raw_ingestion/config.json')
    dbconn.validate()

if __name__ == '__main__': main()
