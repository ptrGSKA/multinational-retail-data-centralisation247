from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
from cred_reader import CredentialReader
from decorator_class import DecoratorClass
import pandas as pd
import os
import tabula
import requests
import boto3

import faulthandler
faulthandler.enable()

# Class definition of the Data Extractor class for extraction of data from multiple sources.
class DataExtractor:
    '''
    The class is used to extract data from various sources for the project, including csv files, API and AWS S3 bucket.

    Parameters:

    
    Args:
        

    Methods:
        __init__():
        read_rds_table():
        retrieve_pdf_data():
        list_number_of_stores():
        retrieve_stores_data():
        extract_from_s3():
        extract_json_data():
        extract_first_source():
        extract_second_source():
        extract_third_source():
        extract_forth_source():
        extract_fifth_source():
        extract_sixth_source():
    '''

    # Class constructor
    def __init__(self) -> None:
        '''
        The constructor initializes the attributes that necessary for the class instances.
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

        Returns:
            Returns a pandas dataframe.
        '''
        try:
            #df_card = tabula.read_pdf(link, pages = 'all')
            tabula.convert_into(link, os.path.join(self.file_dir, 'user_card_data.csv'), output_format='csv', pages = 'all', stream=True)
            df_card  = pd.read_csv(os.path.join(self.file_dir,'user_card_data.csv'))

            print('User card data has been sucessfully extracted from AWS S3 pdf document.')

            return df_card
        
        except Exception as e:
            print(e)

    
    def list_number_of_stores(self, endpoint, api_header):
        '''
        This function is to retrieve the number of stores from an API request.

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

        Returns:
            Returns a pandas dataframe.
        '''
        try:
            df_stores = []

            for store in range(0,no_stores):
                store_endp = ''.join([store_endpoint, str(store)])
                s = requests.Session()
                with s.get(store_endp, headers= crds) as response2:
                    if response2.status_code == 200:
                        data = response2.json()
                        df_stores.append(data)
                        print(f'Store number {store} has been downloaded!', end='\r')
                    else:
                        print(f"Request failed with status code: {response2.status_code}")
                        print(f"Response Text: {response2.text}")

            df = pd.DataFrame(df_stores)
            df.to_csv(os.path.join(self.file_dir,'stores.csv'), sep=',', header = True)
            
            print('Stores data has been sucessfully extracted from the API source.')
            return df
        
        except Exception as e:
            print(e)


    def extract_from_s3(self, address):
        '''
        This function is to retrieve the data from AWS S3 bucket.

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


    @DecoratorClass
    def extract_first_source(self):
        user_df = self.read_rds_table(self.remote_rds_db.engine, self.tables[1])
        clean_user_df = self.cleaning.clean_user_data(user_df)
        self.local_rds_db.upload_to_db(clean_user_df, 'dim_users')

    @DecoratorClass
    def extract_second_source(self):
        card_df = self.retrieve_pdf_data(self.pdf_data)
        clean_card_df = self.cleaning.clean_card_data(card_df)
        self.local_rds_db.upload_to_db(clean_card_df, 'dim_card_details')

    @DecoratorClass
    def extract_third_source(self):
        stores = self.list_number_of_stores(self.num_of_stores, self.creds)
        store_df = self.retrieve_stores_data(self.store_endpoint, stores, self.creds)
        clean_store_df = self.cleaning.clean_data_store(store_df)
        self.local_rds_db.upload_to_db(clean_store_df, 'dim_store_details')

    @DecoratorClass
    def extract_forth_source(self):
        products_df = self.extract_from_s3(self.s3_address)
        products_conversion = self.cleaning.convert_product_weights(products_df)
        clean_products_df = self.cleaning.clean_products_data(products_conversion)
        self.local_rds_db.upload_to_db(clean_products_df, 'dim_products')

    @DecoratorClass
    def extract_fifth_source(self):
        print(f'Table {self.tables[2]} is being extracted.')
        orders_df = self.read_rds_table(self.remote_rds_db.engine, self.tables[2])
        clean_orders_df = self.cleaning.clean_orders_data(orders_df)
        self.local_rds_db.upload_to_db(clean_orders_df, 'orders_table')

    @DecoratorClass
    def extract_sixth_source(self):
        sales_date_df = self.extract_json_data(self.json_address)
        clean_sales_date_df = self.cleaning.clean_sales_date(sales_date_df)
        self.local_rds_db.upload_to_db(clean_sales_date_df, 'dim_date_times')
