import os
import yaml
import psycopg2
from sqlalchemy import create_engine, inspect
from cred_reader import CredentialReader


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

    def list_db_tables(self):
        '''
        This function returns the available tables in the AWS RDS database.

        Returns:
            Returns a list of database tables.
        '''

        # Initializing a database engine to the AWS database
        self.__init_db_engine('AWS')
        
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
        

        
        
