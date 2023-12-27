from sqlalchemy import create_engine, inspect, text
from sqlalchemy_utils import database_exists
from cred_reader import CredentialReader
import os
import yaml
import psycopg2


# Class definition of the Database Connector class to communicate with the database.
class DatabaseConnector:
    '''
    The class is used to communicate with the database to query and upload data to and from it.

    Parameters:

    
    Args:
        

    Methods:
        __init__(self): Class constructor.
        __read_db_creds(self): Function that reads database credentials.
        __init_db_engine(self): Function to initialize database connection.
        list_db_tables(self): Function that reads the available tables from the database.
    '''

    # Class constructor
    def __init__(self) -> None:
        '''
        The constructor initializes the attributes that necessary for the class instances.
        '''
        self.path =  os.path.realpath(__file__)
        self.dir = os.path.dirname(self.path)
        self.db_dir = self.dir.replace('source','database')

        self.credentials = CredentialReader()

    def __read_db_creds(self, location):
        '''
        This function is to read database credentials from a yaml file.

        Returns:
            Returns the credentials from the yaml file
        '''

        db_creds = self.credentials.credential_extraction('Database', location)

        return db_creds


    def __init_db_engine(self, location):
        '''
        This function is to create a database engine with the returned credentials from __read_db_creds method.

        Returns:
            Returns a database engine.
        '''

        creds = self.__read_db_creds(location)
        creds_list = list(creds.values())

        HOST = creds_list[0]
        PASSWORD = creds_list[1]
        USER = creds_list[2]
        DATABASE = creds_list[3]
        PORT = creds_list[4]
        DATABASE_TYPE = creds_list[5]
        DBAPI = creds_list[6]

        self.engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")

        return self.engine

    def list_db_tables(self, location):
        '''
        This function returns the available tables in the AWS RDS database.

        Returns:
            Returns a list of database tables.
        '''

        # Initializing a database engine to the AWS database
        self.__init_db_engine(location)
        
        with self.engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
             self.inspector = inspect(self.engine)
             self.db_tables = self.inspector.get_table_names()

        return self.db_tables
    
    def upload_to_db(self, dataframe_to_upload, table_name_to_upload):
        '''
        This function uploads a pandas dataframe to a local postgresql database.

        Returns:
            None.
        '''
        try:
            # Initializing a database engine to the LOCAL database
            self.__init_db_engine('LOCAL')

            dataframe_to_upload.to_sql(table_name_to_upload, self.engine, if_exists='replace')

            print(f'Data has been sucessfully uploaded to the {table_name_to_upload} table in the local database.')

        except Exception as e:
            print(e)
        
    def check_db_existence(self):
        '''
        This function checks the existence of the database.

        Returns:
            Boolean.
        '''
        print('Checking the existence of the database. \n')
        
        #db_engine = 
        self.__init_db_engine('LOCAL')

        if database_exists(self.engine.url):
            print('The local databse exists. \n')
        else:
            self.create_database()

        return database_exists(self.engine.url)

    def check_db_tables(self):
        '''
        This function checks the existence of the required tables in the LOCAL database.

        Returns:
            List with the difference of the required and currently existing tables.
        '''
        
        tables = self.list_db_tables('LOCAL')

        required_tables = ['dim_users', 'dim_card_details', 'dim_store_details', 'dim_products', 'dim_orders', 'dim_date_times']
        existing_tables = []

        for table_name in tables:
            existing_tables.append(table_name)
        
        intersection = set(required_tables).intersection(existing_tables)
        difference = set(required_tables).difference(existing_tables)
        
        if len(intersection) > 0 and len(difference) == 0:
            for table in intersection:
                print(f'The following table {table} exists in the database.')
        elif len(intersection) > 0 and len(difference) > 0:
            for table in intersection:
                print(f'The following table {table} exists in the database.')
                print('\n')
            for table in difference:
                print(f'The following table {table} doesn\'t exists in the database and need to be created first.')
        else:
            for table in difference:
                print(f'The following table {table} need to be created first.')
        
        return list(difference)
    

    def create_database(self):
        '''
        This function takes a file_name and return it's content.

        Returns:
            String - SQL query.
        '''
        
        db_file = 'create_db.sql'
        query = self.get_sql_files(db_file)

        self.__execute_db_query(query)

        print('Database has been sucessfully created!')
    
    
    def create_tables(self, missing_tables):
        '''
        This function takes a list and it creates the required tables in the database based on the entries in the list if it's not empty.

        Returns:
            List of existing tables.
        '''
        
        for table in missing_tables:
            full_file_name = f'create_{table}.sql'
            query = self.get_sql_files(full_file_name)
            self.__execute_db_query(query)
            print(f'Table {table} has been created.')
            
        return self.check_db_tables()
    

    def __execute_db_query(self, query):
        '''
        This function takes a list and it creates the required tables in the database based on the entries in the list if it's not empty.

        Returns:
            List of existing tables.
        '''

        with self.engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
            sql = text(f"""{query}""")
            conn.execute(sql)
    

    def get_sql_files(self, file_name):
        '''
        This function takes a file_name and return it's content.

        Returns:
            String - SQL query.
        '''

        with open(os.path.join(self.db_dir,file_name), mode = 'r') as file:
            return file.read()
    

#for column in inspector.get_columns(table_name):
#    print("Column: %s" % column['name'])