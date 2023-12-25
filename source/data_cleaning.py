import os
import pandas as pd
import numpy as np
import re


# Class definition of the Data Cleaning class for cleaning data for the project.
class DataCleaning:
    '''
    The class is used to clean the data extracted from various sources.

    Parameters:

    
    Args:
        

    Methods:
        clean_user_data(): 
        clean_card_data(): 
        clean_data_store(): 
    '''

    # Class constructor
    def __init__(self) -> None:
        '''
        The constructor initializes the attributes that necessary for the class instances.
        '''
        self.path =  os.path.realpath(__file__)
        self.dir = os.path.dirname(self.path)
        self.file_dir = self.dir.replace('source','data_files')

    def clean_user_data(self):
        '''
        The function cleans the legacy user dataframe. Removes NULL values, leading or trailing white spaces, special characters,
        removes unnecessary columns and rows.
        '''

        df = pd.read_csv(os.path.join(self.file_dir,'legacy_users.csv'), header = 0, index_col = 'index')

        # Drop rows where all of them are NaN's
        mask = df[df.isnull().any(axis=1)]
        df = df[~df.index.isin(mask.index)]
        
        # Drop column Unnamed: 0, it's a duplicate of the index column
        df = df.drop(labels = 'Unnamed: 0', axis = 1)

        # Removing leading and trailing white spaces if there is any from all the columns
        for col in df.columns:
            df[col] = df[col].str.strip()

        # Removing digits and special characters from the first name, last name, country and country code columns as these should not contain anything else just alphabetical characters.
        pattern = "[^A-Za-z\s]+"

        df['first_name'] = df['first_name'].apply(lambda x: re.sub(pattern, '', x))
        df['last_name'] = df['last_name'].apply(lambda x: re.sub(pattern, '', x))
        df['country'] = df['country'].apply(lambda x: re.sub(pattern, '', x))
        df['country_code'] = df['country_code'].apply(lambda x: re.sub(pattern, '', x))

        # Removing \n (new line) characters from address
        df['address'] = df['address'].str.replace('\n', ' ')

        # Removing rows where there is no real data present only gibberish, using the country column to find those rows as there are onyl three countries present in the dataframe.
        countries = ['United Kingdom', 'United States', 'Germany']
        mask2 = df[~df['country'].isin(countries)]
        df = df[~df.index.isin(mask2.index)]

        # Fixing the country codes for the countries
        df['country_code'] = df['country_code'].str.replace('GGB', 'GB')
        df['country_code'] = pd.Categorical(df['country_code'])

        # Converting the date of borth and joined data columns into datetime object.
        df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], format = 'mixed')
        df['join_date'] = pd.to_datetime(df['join_date'], format = 'mixed')

        # Removing everything from the phone number columns that is not a digit.
        phone_pattern = '[A-Za-z\s\W]'
        df['phone_number'] = df['phone_number'].apply(lambda x: re.sub(phone_pattern, '', x))
        
        # Cleaning email address
        df['email_address'] = df['email_address'].str.replace('@@', '@')

        print('Data legacy_users dataframe has been sucessfully cleaned.')

        return df
    
    def clean_card_data(self):
        '''
        The function cleans the user card dataframe. Removes NULL values, special characters,
        removes unnecessary rows and imputes values where it's possible.
        '''

        df = pd.read_csv(os.path.join(self.file_dir,'user_card_data.csv'), header = 0)

        # Removes null value where all of the entries in a row are null.
        mask = df[df.isnull().all(axis=1)]
        df = df[~df.index.isin(mask.index)]

        # Removes the rows where they were headings in a pds format.
        mask2 = df[df['card_number'].str.contains('card_number')]
        df = df[~df.index.isin(mask2.index)]

        # Removing the question mark from the card_nuber column.
        df['card_number'] = df['card_number'].str.replace('?','')

        # FInds entries with alphabetical characters in the card number column and replaces them with a dash (marks them) and then any entry that contains dashes is being removed as those are not valid cards.
        pattern = "[A-Za-z]"
        df['card_number'] = df['card_number'].apply(lambda x: re.sub(pattern, '-', x))

        mask3 = df[~df['card_number'].str.isdigit()]
        gibberish = mask3['card_number'].str.contains('-')
        df = df[~df.index.isin(gibberish.index[gibberish == True])]

        # Imputing the missing expiry date where they are present in the card number column and fixing the card numbers.
        df.loc[df['expiry_date'].isnull(), 'expiry_date'] = df.loc[df['expiry_date'].isnull()]['card_number'].str.split(' ').str[1]
        df.loc[df['card_number'].str.contains('/'), 'card_number'] = df.loc[df['card_number'].str.contains('/')]['card_number'].str.split(' ').str[0]

        # Converting the columns into their final types.
        df['date_payment_confirmed'] = pd.to_datetime(df['date_payment_confirmed'], format = 'mixed')
        df['expiry_date'] = pd.to_datetime(df['expiry_date'], format = '%m/%y')
        df['card_number'] = df['card_number'].astype('int64')

        print('Data user_card_data dataframe has been sucessfully cleaned.')

        return df
    
    def clean_data_store(self):
        '''
        The function cleans the stores dataframe. Removes NULL values, special characters,
        removes unnecessary rows and imputes values where necessary.
        '''
        
        df = pd.read_csv(os.path.join(self.file_dir,'stores.csv'), header = 0)

        # Dropping the unnecessary column Unnamed: 0 and lat where over 90% of the data is missing.
        df = df.drop(labels = ['Unnamed: 0','lat'], axis = 1)

        # Removing the newline characters from the address column.
        df['address'] = df['address'].str.replace('\n', ' ')

        # Cleaning the continent column
        df.loc[df['continent'] == 'eeEurope', 'continent'] = 'Europe'
        df.loc[df['continent'] == 'eeAmerica', 'continent'] = 'America'

        # Removing gibberish data based on the continent column.
        continents = ['Europe', 'America']
        df = df[df['continent'].isin(continents)]

        # Removing alphabetical and special characters from the staff numbers column.
        df['staff_numbers'] = df['staff_numbers'].apply(lambda x: re.sub('[A-Za-z\W]', '', x))

        # Removing the dashes from the city names.
        df['locality'] = df['locality'].str.replace('-',' ')

        # There is one online store where the data need to be imputed.
        df.loc[df['address'].isnull(), ['longitude', 'latitude']] = '00.00'
        df.loc[df['address'].isnull(), ['address', 'locality']] = 'Online Store'

        # Converting the columns into the correct type.
        df['opening_date'] = pd.to_datetime(df['opening_date'], format = 'mixed')
        df['longitude'] = df['longitude'].astype('float32')
        df['latitude'] = df['latitude'].astype('float32')
        df['staff_numbers'] = df['staff_numbers'].astype('int16')
        df['country_code'] = df['country_code'].astype('category')
        df['continent'] = df['continent'].astype('category')
        df['store_type'] = df['store_type'].astype('category')

        print('Data store dataframe has been sucessfully cleaned.')

        return df

    def convert_product_weights(self, dataframe):
        '''
        The function converts the weight of products into a unified weight.

        Return:
            Returns the product dataframe.
        '''

        

        pass

    def clean_products_data(self):
        '''
        The function cleans the products dataframe. Removes NULL values, special characters,
        removes unnecessary rows and imputes values where necessary.
        '''
        df = pd.read_csv(os.path.join(self.file_dir,'products.csv'), header = 0)

        print('Data products dataframe has been sucessfully cleaned.')

        return df