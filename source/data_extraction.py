from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
from cred_reader import CredentialReader
import pandas as pd
import os
import tabula
import requests
import boto3

import faulthandler
faulthandler.enable()


class DataExtractor:
    '''
    The class is used to extract data from various sources for the project, including csv files, API and AWS S3 bucket.

    Methods:
        __init__(): class constructor
        read_rds_table(): retrieves data from remote database
        retrieve_pdf_data(): extracts data from pdf source file
        list_number_of_stores(): lists the number of sources
        retrieve_stores_data(): downloads data via API request
        extract_from_s3(): extracts data from S3 bucket via boto
        extract_json_data(): extracts data via an API request
        extract_first_source(): first source extraction, cleaning and uploading process
        extract_second_source(): second source extraction, cleaning and uploading process
        extract_third_source(): third source extraction, cleaning and uploading process
        extract_forth_source(): forth source extraction, cleaning and uploading process
        extract_fifth_source(): fifth source extraction, cleaning and uploading process
        extract_sixth_source(): sixth source extraction, cleaning and uploading process
    '''

    # Class constructor
    def __init__(self) -> None:
        '''
        The constructor initializes the attributes that necessary for the class instances.

        Args:
            path: real path to the source file where it was called
            dir: the directory of the file
            file_dir: full path pointing to the directory of the data_files in any operating system
            remote_rds_db: class instance of DatabaseConnector
            tables: tables present in the remote AWS source
            credentials: class instance of CredentialReader
            cleaning: class instance of Datacleaning
            local_rds_db: class instance if DatabaseConnector
            pdf_data: link to source
            num_of_stores: link to source
            store_endpoint: link to source
            s3_address: link to source
            json_address: link to source
            creds: API credentials

        Returns:
            None
        '''
        self.path =  os.path.realpath(__file__)
        self.dir = os.path.dirname(self.path)
        self.file_dir = self.dir.replace('source','data_files')

        self.remote_rds_db = DatabaseConnector()
        self.tables = self.remote_rds_db.list_db_tables('AWS')

        self.credentials = CredentialReader()
        self.cleaning = DataCleaning()
        self.local_rds_db = DatabaseConnector()

        self.pdf_data = self.credentials.credential_extraction('Links','Link')['pdf_data']
        self.num_of_stores = self.credentials.credential_extraction('Links','Link')['num_of_stores']
        self.store_endpoint = self.credentials.credential_extraction('Links','Link')['store_endpoint']
        self.s3_address = self.credentials.credential_extraction('Links','Link')['s3_address']
        self.json_address = self.credentials.credential_extraction('Links','Link')['json_address']
        self.creds = self.credentials.credential_extraction('Creds', 'API')
        
                
    def read_rds_table(self, remote_rds_db, table_name):
        '''
        This function is to read database tables from the AWS RDS database.

        Args:
            remote_rds_db: DatabaseConnector class that po
            table_name: the table to extract

        Returns:
            Returns a pandas dataframe.
        '''
        try:
            df = pd.read_sql_table(table_name, remote_rds_db)
            df.to_csv(os.path.join(self.file_dir,f'{table_name}.csv'), sep=',', header = True)

            print(f'Data {table_name} has been sucessfully extracted from database.')

            return df           
        
        except Exception as e:
            print(e)
    
        

    def retrieve_pdf_data(self, link):
        '''
        This function is to retrieve a pdf document from AWS S3.

        Args:
            link: link to source

        Returns:
            Returns a pandas dataframe.
        '''
        try:
            tabula.convert_into(link, os.path.join(self.file_dir, 'user_card_data.csv'), output_format='csv', pages = 'all', stream=True)
            df_card  = pd.read_csv(os.path.join(self.file_dir,'user_card_data.csv'))

            print('User card data has been sucessfully extracted from AWS S3 pdf document.')

            return df_card
        
        except Exception as e:
            print(e)

    
    def list_number_of_stores(self, endpoint, api_header):
        '''
        This function is to retrieve the number of stores from an API request.

        Args:
            endpoint: link to the source
            api_header: API credentials

        Returns:
            Returns a string.
        '''
        try:
            response = requests.get(endpoint, headers=api_header)

            if response.status_code == 200:
                data = response.json()
            else:
                print(f"Request failed with status code: {response.status_code}")
                print(f"Response Text: {response.text}")

            return data['number_stores']
        
        except Exception as e:
            print(e)
            

    def retrieve_stores_data(self, store_endpoint, no_stores, crds):
        '''
        This function is to retrieve the data from an API request and creates a pandas dataframe.

        Args:
            store_endpoint: link to source
            no_stores: number of files (stores) at the endpoint
            crds: API credentials

        Returns:
            Returns a pandas dataframe.
        '''

        try:
            df_stores = []

            for store in range(0,no_stores):
                store_endp = ''.join([store_endpoint, str(store)])
                s = requests.Session()
                with s.get(store_endp, headers= crds) as response:
                    if response.status_code == 200:
                        data = response.json()
                        df_stores.append(data)
                        print(f'Store number {store} has been downloaded!', end='\r')
                    else:
                        print(f"Request failed with status code: {response.status_code}")
                        print(f"Response Text: {response.text}")

            df = pd.DataFrame(df_stores)
            df.to_csv(os.path.join(self.file_dir,'stores.csv'), sep=',', header = True)
            
            print('Stores data has been sucessfully extracted from the API source.')
            return df
        
        except Exception as e:
            print(e)


    def extract_from_s3(self, address):
        '''
        This function is to retrieve the data from AWS S3 bucket.

        Args:
            address: link to source

        Returns:
            Returns a pandas dataframe.
        '''
        try:
            adrs = address.split('/')

            s3 = boto3.client('s3')
            s3.download_file(adrs[2], '/'.join(adrs[3:]), os.path.join(self.file_dir,'products.csv'))

            df = pd.read_csv(os.path.join(self.file_dir,'products.csv'))

            print('Products data has been sucessfully extracted from AWS S3 source.')
            return df
        
        except Exception as e:
            print(e)


    def extract_json_data(self, address):
        '''
        This function is to retrieve the data from AWS via an API request.

        Args:
            address: link to source

        Returns:
            Returns a pandas dataframe.
        '''
        try:
            response = requests.get(address)
            repos = response.json()

            df = pd.DataFrame(repos)
            df.to_csv(os.path.join(self.file_dir,'sale_date.csv'), header = True)

            print('Sales date data been sucessfully extracted from AWS S3 source.')
            return df

        except Exception as e:
            print(e)
