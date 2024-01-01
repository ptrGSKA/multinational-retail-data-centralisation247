from sqlalchemy import create_engine, inspect, text
from sqlalchemy_utils import database_exists
from cred_reader import CredentialReader
import os
import pandas as pd


# Class definition of the DatabaseConnector.
class DatabaseConnector:
    '''
    The class is used to communicate with the database to query and upload data to and from it.

    Methods:
        __init__(self): Class constructor.
        __read_db_creds(self): Function that reads database credentials.
        __init_db_engine(self): Function to initialize database connection.
        list_db_tables(self): Function that reads the tables from the database.
        upload_to_db(): Funtion that uploads data into the local database
        check_db_existence(): Funntion to check the existance of database
        check_db_tables(): Function that perform a table check
        create_database(): Function that creates the detabase if necessary
        create_tables(): Function that creates database tables
        __execute_db_query(): Function that executes raw queries
        __execute_db_query_pandas(): Function that executes raw queries with pandas module
        __get_sql_files(): Function that retrieves that content of a file
        alter_tables_data_types(): Function that retrieves and executes the table data type alteration queries
        alter_tables_keys(): Function that retrieves and executes the table key alteration queries
        select_query(): Function that retrieves and executes the selection queries
    '''

    # Class constructor
    def __init__(self) -> None:
        '''
        The constructor initializes the attributes that necessary for the class instances.

        Args:
            path: real path to the source file where it was called
            dir: the directory of the file
            db_dir_create: full path pointing to the create_tables directory in any operating system that created the tables if not exists
            db_dir_alter: full path pointing to the alter_tables directory in any operating system that contains the queries for the project
            db_dir_select: full path pointing to the select_query directory in any operating system that contains the queries for the project
            db_dir_result: full path pointing to the query_results directory in any operating system where the query results is being saved as a csv file
            credentials: the class instance of the CredentialReader

        Returns:
            None
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

        Args:
            location: the subkey within the Database yaml document.

        Returns:
            Returns the credentials from the yaml file
        '''

        db_creds = self.credentials.credential_extraction('Database', location)

        return db_creds


    def __init_db_engine(self, location):
        '''
        This function is to create a database engine with the returned credentials from __read_db_creds method.

        Args:
            location: the subkey within the Database yaml document.

        Returns:
            Returns a database engine.
        '''

        # Credential values as a list and assigned to the database engine creation variables.
        creds = self.__read_db_creds(location)
        creds_list = list(creds.values())

        HOST = creds_list[0]
        PASSWORD = creds_list[1]
        USER = creds_list[2]
        DATABASE = creds_list[3]
        PORT = creds_list[4]
        DATABASE_TYPE = creds_list[5]
        DBAPI = creds_list[6]

        # Cheking for db creation or returning an engine pointing to a database.
        if location != 'NEW_DB':
            self.engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        else:
            self.engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}")

        return self.engine


    def list_db_tables(self, location):
        '''
        This function returns the available tables in the AWS RDS database.

        Args:
            location: the subkey within the Database yaml document.

        Returns:
            Returns a list of database tables.
        '''

        # Initializing a database engine to the required database and inspects the existing tables.
        self.__init_db_engine(location)
        
        with self.engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
             self.inspector = inspect(self.engine)
             self.db_tables = self.inspector.get_table_names()

        return self.db_tables
    

    def upload_to_db(self, dataframe_to_upload, table_name_to_upload):
        '''
        This function uploads a pandas dataframe to a local postgresql database.

        Args:
            dataframe_to_upload: the cleaned dataframe that is being uploaded to the database
            table_name_to_upload: the database table where the data is being uploaded to

        Returns:
            None.
        '''
        try:
            # Initializing a database engine to the LOCAL database
            self.__init_db_engine('LOCAL')

            # Uploading the dataframe by appending it to an existing table rather then creating a new table.
            dataframe_to_upload.to_sql(table_name_to_upload, self.engine, if_exists='append', index = False)

            print(f'Data has been sucessfully uploaded to the {table_name_to_upload} table in the local database.')

        except Exception as e:
            print(e)
        

    def check_db_existence(self):
        '''
        This function checks the existence of the LOCAL database.

        Returns:
            Boolean.
        '''
        print('Checking the existence of the local database. \n')
        
        # Engine to the local database
        self.__init_db_engine('LOCAL')
        
        # Checking the existence of the database
        if database_exists(self.engine.url):
            print('The local databse exists. \n')
        else:
            print('The local database doesn\'t exists!')

        return database_exists(self.engine.url)


    def check_db_tables(self):
        '''
        This function checks the existence of the required tables in the LOCAL database.

        Returns:
            List with the difference of the required and currently existing tables.
        '''
        
        # List of the tables in the local database
                    #tables = self.list_db_tables('LOCAL')

        # List of tables required to the project
        db_tables = self.credentials.credential_extraction('Database', 'TABLES')
        
        self.required_tables = list(db_tables.values())
        existing_tables = self.list_db_tables('LOCAL')

                    #for table_name in tables:
                    #    existing_tables.append(table_name)
        
        # Intersection and difference of the existing and required tables
        intersection = set(self.required_tables).intersection(existing_tables)
        difference = set(self.required_tables).difference(existing_tables)
        
        # Performs the check and informs the user about it.
        if len(intersection) > 0 and len(difference) == 0:
            print('The following tables already exists in the database.')
            for index, table in enumerate(intersection):
                print(f'{index+1}. -----> {table}')
        elif len(intersection) > 0 and len(difference) > 0:
            print('The following tables already exists in the database.')
            for index,table in enumerate(intersection):
                print(f'{index+1}. -----> {table}')
                print('\n')
            print('The following tables doesn\'t exists in the database and need to be created first.')
            for index,table in enumerate(difference):
                print(f'{index+1}. -----> {table}')
        else:
            print(f'The following tables need to be created first.')
            for index, table in enumerate(difference):
                print(f'{index+1}. -----> {table}')
        
        return list(difference)
    

    def create_database(self):
        '''
        This function will perform the creation of an empty database.

        Returns:
            None
        '''
        
        # Engine to create an empty database.
        self.__init_db_engine('NEW_DB')

        # The sql file name, calling the files retrieval and executing the raw query.
        db_file = 'create_db.sql'
        query = self.__get_sql_files(db_file)
        self.__execute_db_query(query)

        print('Database has been sucessfully created!')
    
    def create_tables(self, missing_tables):
        '''
        This function takes a list and it creates the required tables in the database.

        Args:
            missing_tables: tables that are not present in the database but required to the project

        Returns:
            None.
        '''

        # Takes the list to read the table names that needs to be created, gets the file and executes the query.
        print(f'The following table has been created.')
        for index,table in enumerate(missing_tables):
            full_file_name = f'create_{table}.sql'
            query = self.__get_sql_files(full_file_name)
            self.__execute_db_query(query)
            print(f'{index}. -----> {table}')
                

    def __execute_db_query(self, query):
        '''
        This function takes a raw query and executes it using sqlalchemy module.

        Args:
        query: the raw query to execute on the database

        Returns:
            The result of a query.
        '''

        with self.engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
            sql = text(f"""{query}""")
            result = conn.execute(sql)

            return result
        
    
    def __execute_db_query_pandas(self,query):
        '''
        This function takes a raw query and executes it using the pandas module.
        Used only with the select_query method to provide saving the result as a csv file.

        Args:
            query: the raw query to execute on the database

        Returns:
            The result of a query.
        '''

        with self.engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
            sql = text(f"""{query}""")
            result = pd.read_sql_query(sql,conn)

            return result



    def __get_sql_files(self, file_name):
        '''
        This function takes a file_name and return it's content.

        Args:
            file_name: the file name to retrieve

        Returns:
            String - SQL query.
        '''

        dir = ''

        # Based on the file names first split, either create, alter or select updates the dir variable
        if file_name.split('_')[0] == 'create':
            dir = self.db_dir_create
        elif file_name.split('_')[0] == 'alter':
            dir = self.db_dir_alter
        elif file_name.split('_')[0] == 'select':
            dir = self.db_dir_select

        # Based on the dir variables it creates the full path to the file and reads it's content.
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

        # Names of the files.
        alter_keys = ['tables_primary_key', 'tables_foreign_key']

        for table in alter_keys:
            full_file_name = f'alter_{table}.sql'
            query = self.__get_sql_files(full_file_name)
            self.__execute_db_query(query)
            print('Tables have been altered according to the sql file.')

    
    def select_query(self):
        '''
        This function reads the files from the select_query directory and performs the execution of each file and the results are saved in the query_result diectory as csv files.
        Used with the __execute_db_query_pandas() method only to let to save the file as a csv. It also prints the result to the terminal.
 
        Returns:
            None.
        '''
        
        # List the content of the directory
        query_files = os.listdir(self.db_dir_select)

        # Read the files, and execute the query, save it as a csv file while also printing the result to the terminal.
        for q_file in query_files:
            if q_file.split('_')[0] == 'select':
                query = self.__get_sql_files(q_file)
                result = self.__execute_db_query_pandas(query)

                # Save query to a csv file.
                file_name = q_file.split('.')[0]
                result.to_csv(os.path.join(self.db_dir_result, file_name+'.csv'), sep=',', header = True)
                print(f'The result of the query has been written in the {file_name}.csv file in the query_results folder.\n')

                # Print result to terminal <optional>
                print(f'Query number {q_file.split("_")[2].split(".")[0]} result.')
                print('-'*40)
                print('*'*40)
                print(result)
                print('*'*40)
                print('\n')   
            else:
                print(' The file is not a selection query!')