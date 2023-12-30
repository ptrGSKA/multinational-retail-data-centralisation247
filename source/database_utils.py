from sqlalchemy import create_engine, inspect, text
from sqlalchemy_utils import database_exists
from cred_reader import CredentialReader
import os
import pandas as pd


# Class definition of the DatabaseConnector class that perform the communication with the database.
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
        upload_to_db():
        check_db_existence():
        check_db_tables():
        create_database():
        create_tables():
        __execute_db_query():
        __execute_db_query_pandas():
        __get_sql_files():
        alter_tables_data_types():
        alter_tables_keys():
        select_query():
    '''

    # Class constructor
    def __init__(self) -> None:
        '''
        The constructor initializes the attributes that necessary for the class instances.
        '''
        self.path =  os.path.realpath(__file__)
        self.dir = os.path.dirname(self.path)
        self.db_dir_create = self.dir.replace('source','database/create_tables')
        self.db_dir_alter = self.dir.replace('source','database/alter_tables')
        self.db_dir_select = self.dir.replace('source','database/select_query')
        self.db_dir_result = self.dir.replace('source', 'database/query_results')

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

            dataframe_to_upload.to_sql(table_name_to_upload, self.engine, if_exists='append')

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

        db_tables = self.credentials.credential_extraction('Database', 'TABLES')
        
        self.required_tables = list(db_tables.values())
        existing_tables = []

        for table_name in tables:
            existing_tables.append(table_name)
        
        intersection = set(self.required_tables).intersection(existing_tables)
        difference = set(self.required_tables).difference(existing_tables)
        
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
        query = self.__get_sql_files(db_file)
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
            query = self.__get_sql_files(full_file_name)
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
            result = conn.execute(sql)

            return result
        
    
    def __execute_db_query_pandas(self,query):
        '''
        This function takes a list and it creates the required tables in the database based on the entries in the list if it's not empty.

        Returns:
            List of existing tables.
        '''

        with self.engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
            sql = text(f"""{query}""")
            result = pd.read_sql_query(sql,conn)

            return result



    def __get_sql_files(self, file_name):
        '''
        This function takes a file_name and return it's content.

        Returns:
            String - SQL query.
        '''

        dir = ''

        if file_name.split('_')[0] == 'create':
            dir = self.db_dir_create
        elif file_name.split('_')[0] == 'alter':
            dir = self.db_dir_alter
        elif file_name.split('_')[0] == 'select':
            dir = self.db_dir_select

        with open(os.path.join(dir, file_name), mode = 'r') as file:
            return file.read()
        
    def alter_tables_data_types(self):
        '''
        This function performs the execution of the data type alteration queries.

        Returns:
            None.
        '''

        for table in self.required_tables:
            full_file_name = f'alter_{table}.sql'
            query = self.__get_sql_files(full_file_name)
            self.__execute_db_query(query)
            print(f'Table {table} has been altered.')


    def alter_tables_keys(self):
        '''
        This function performs the execution of the tables primary and foreign key creation queries.

        Returns:
            None.
        '''

        alter_keys = ['tables_primary_key', 'tables_foreign_key']

        for table in alter_keys:
            full_file_name = f'alter_{table}.sql'
            query = self.__get_sql_files(full_file_name)
            self.__execute_db_query(query)
            print('Tables have been altered according to the sql file.')

    
    def select_query(self):
        '''
        This function reads the files from the select_query directory and performs the execution of each file and the results are saved in the query_result diectory as csv files.

        Returns:
            None.
        '''
        
        query_files = os.listdir(self.db_dir_select)

        for q_file in query_files:
            if q_file.split('_')[0] == 'select':
                query = self.__get_sql_files(q_file)
                result = self.__execute_db_query_pandas(query)

                file_name = q_file.split('.')[0]
                result.to_csv(os.path.join(self.db_dir_result, file_name+'.csv'), sep=',', header = True)
                #self.__write_query_to_txt(file_name, result.fetchall())
                print(f'The result of the query has been written in the {file_name}.csv file in the query_results folder.')
            else:
                print(' The file is not a selection query!')
                exit
        


#for column in inspector.get_columns(table_name):
#    print("Column: %s" % column['name'])