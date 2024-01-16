import os
import pandas as pd
import numpy as np
import re


class DataCleaning:
    '''
    The class is used to clean the data extracted from various sources. Each method cleans 
    exactly one dataframe and return it.      

    Methods:
        __init__(): class constructor
        clean_user_data(): the function takes a dataframe as an argument and performs the legacy_user data cleaning
        clean_card_data(): the function takes a dataframe as an argument and performs the user_card data cleaning
        clean_data_store(): the function takes a dataframe as an argument and performs the stores data cleaning
        convert_product_weights(): the function takes a dataframe as an argument and performs the conversion of each weight type to kg
        clean_products_data(): the function takes a dataframe as an argument and performs the rest of the products data cleaning
        clean_orders_data(): the function takes a dataframe as an argument and performs the orders table cleaning
        clean_sales_data(): the function takes a dataframe as an argument and performs the sales data cleaning

    '''

    def __init__(self) -> None:
        '''
        The constructor initializes the attributes that necessary for the class instances.

        Args:
            path: real path to the source file where it was called
            dir: the directory of the file
            file_dir: full path pointing to the directory of the data_files in any operating system

        Returns:
            None
        '''
        self.path =  os.path.realpath(__file__)
        self.dir = os.path.dirname(self.path)
        self.file_dir = self.dir.replace('source','data_files')

    def clean_user_data(self, dataframe):
        '''
        The function takes a dataframe as an argument and performs the cleaning of the legacy user dataframe.
        Removes NULL values, special characters if any present , removes unnecessary columns and rows.

        Args:
            dataframe: the dataframe to clean
        
        Returns:
            The cleaned dataframe.
        '''

        df = dataframe

        # Drop rows where all of them are NaN's
        mask = df[df.isnull().any(axis=1)]
        df = df[~df.index.isin(mask.index)]
        
        # Drop column Unnamed: 0, it's a duplicate of the index column
        df = df.drop(labels = 'index', axis = 1)


        # Removing digits and special characters from the first name, last name, country and country code columns if any as these should not contain anything else just alphabetical characters.
        pattern = "[^A-Za-z\s]+"

        df.loc[:,'first_name'] = df['first_name'].apply(lambda x: re.sub(pattern, '', x))
        df.loc[:,'last_name'] = df['last_name'].apply(lambda x: re.sub(pattern, '', x))
        df.loc[:,'country'] = df['country'].apply(lambda x: re.sub(pattern, '', x))
        df.loc[:,'country_code'] = df['country_code'].apply(lambda x: re.sub(pattern, '', x))

        # Removing \n (new line) characters from address
        df.loc[:,'address'] = df['address'].str.replace('\n', ' ')

        # Removing rows where there is no real data present only gibberish, using the country column to find those rows as there are onyl three countries present in the dataframe.
        countries = ['United Kingdom', 'United States', 'Germany']
        mask2 = df[~df['country'].isin(countries)]
        df = df[~df.index.isin(mask2.index)]

        # Fixing the country codes for the countries
        df.loc[:,'country_code'] = df['country_code'].str.replace('GGB', 'GB')
        df['country_code'] = pd.Categorical(df['country_code'])

        # Converting the date of borth and joined data columns into datetime object.
        df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], format = 'mixed')
        df['join_date'] = pd.to_datetime(df['join_date'], format = 'mixed')

        # Removing everything from the phone number columns that is not a digit.
        phone_pattern = '[A-Za-z\s\W]'
        df.loc[:,'phone_number'] = df['phone_number'].apply(lambda x: re.sub(phone_pattern, '', x))
        
        # Cleaning email address
        df.loc[:,'email_address'] = df['email_address'].str.replace('@@', '@')

        print('Data legacy_users dataframe has been sucessfully cleaned.')

        return df
    
    def clean_card_data(self, dataframe):
        '''
        The function takes a dataframe as an argument and performs the cleaning of the users card data dataframe.
        Removes NULL values, special characters if any present, removes unnecessary columns and rows.
        Imputes the data present in the wrong column into the final destination.

        Args:
            dataframe: the dataframe to clean
        
        Returns:
            The cleaned dataframe.
        '''

        df = dataframe

        # Removes null value where all of the entries in a row are null.
        mask = df[df.isnull().all(axis=1)]
        df = df[~df.index.isin(mask.index)]

        # Removes the rows where they were headings in a pds format.
        mask2 = df[df['card_number'].str.contains('card_number')]
        df = df[~df.index.isin(mask2.index)]

        # Removing the question mark from the card_nuber column.
        df['card_number'] = df['card_number'].str.replace('?','')

        # Finds entries with alphabetical characters in the card number column and replaces them with a dash (marks them) and then any entry that contains dashes is being removed as those are not valid cards.
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
                #df['expiry_date'] = pd.to_datetime(df['expiry_date'], format = '%m/%y')    <----UNNECESSARY
        df['card_number'] = df['card_number'].astype('int64')

        print('Data user_card_data dataframe has been sucessfully cleaned.')

        return df
    
    def clean_data_store(self, dataframe):
        '''
        The function takes a dataframe as an argument and performs the cleaning of the stores data dataframe.
        Removes NULL values, special characters if any present and removes unnecessary columns and rows.
        Correcting the continent columns for incorrectly entered values.

        Args:
            dataframe: the dataframe to clean
        
        Returns:
            The cleaned dataframe.
        '''
        
        df = dataframe

        # Dropping the unnecessary column lat where over 90% of the data is missing.
        df = df.drop(labels = ['index', 'lat'], axis = 1)

        # Removes null value where all of the entries in a row are null.
        mask = df[df.isnull().all(axis=1)]
        df = df[~df.index.isin(mask.index)]

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
        df.loc[df['address'].isnull(), ['longitude', 'latitude']] = np.nan
        df.loc[df['address'].isnull(), ['locality', 'address']] = str('Online Store')

        # Converting the columns into the correct type.
        df['opening_date'] = pd.to_datetime(df['opening_date'], format = 'mixed')
        df['staff_numbers'] = df['staff_numbers'].astype('int16')       

        print('Store dataframe has been sucessfully cleaned.')

        return df

    def convert_product_weights(self, dataframe):
        '''
        The function takes a dataframe as an argument and performs the conversion of the weight column.
        It converts all the different units into kg unit.

        Args:
            dataframe: the dataframe to clean
        
        Returns:
            The cleaned dataframe.
        '''

        df = dataframe

        # Removing the gibberish data from the dataframe based on the removed column for easy conversion of weights.
        list = ['Still_avaliable', 'Removed']
        mask2 = df[~df['removed'].isin(list)]
        df = df[~df.index.isin(mask2.index)]

        # Based on the 1:1 conversion rate ml simply replaced with gramm for further conversion.
        df.loc[:,'weight'] = df['weight'].str.replace('ml', 'g') 

        # Separating the weights that not in kg into a subset of the dataframe and removing g.
        not_kg = df[~df['weight'].str.contains('kg')]
        not_kg.loc[:,'weight'] = not_kg['weight'].str.replace('g','')

        # Multipack conversion to single gramm within the subset, after conversion splicing them back into the subset.
        mg = not_kg[not_kg['weight'].str.contains('x')]
        mg.loc[:,'weight']= mg['weight'].apply(lambda x : int(x.split(' ')[0]) * int(x.split(' ')[2]))
        not_kg.loc[not_kg['weight'].index.isin(mg.index), 'weight'] = mg['weight']
        
        # There is a product with oz unit, converting it based on the conversion rate 1oz = 28.35g.
        oz = df[df['weight'].str.contains('oz')]
        oz.loc[:,'weight'] = oz['weight'].str.replace('oz', '')
        oz.loc[:,'weight'] = oz['weight'].apply(lambda x: int(x) * 28.35)
        not_kg.loc[not_kg['weight'].index.isin(oz.index), 'weight'] = oz['weight']

        # Separating Goodmans products as the weight is not correct. Fixing the splicing them back into the subset.
        goodmans = not_kg[not_kg['product_name'].str.contains('Goodmans')]
        goodmans.loc[:,'weight'] = goodmans['weight'].str.lstrip('0.')
        goodmans.loc[:,'weight'] = goodmans['weight'].apply(lambda x: str(x).replace('.','') if len(x) > 4 else x)
        goodmans.loc[:,'weight'] = goodmans['weight'].apply(lambda x: str(x).replace('.5','500'))
        not_kg.loc[not_kg['weight'].index.isin(goodmans.index), 'weight'] = goodmans['weight']

        # Correcting the only value with a weight enging (<whitespace>.)
        not_kg.loc[:,'weight'] = not_kg['weight'].apply(lambda x: str(x).replace(' .', ''))

        # Separating the Disney and Rug products and fixing the weight where it needs to be then splicing them back into the subset.
        disney_rug = not_kg.loc[not_kg['product_name'].str.contains(r'^(?=.*Disney)|(?=.*Rug)')]
        disney_rug.loc[:,'weight'] = disney_rug['weight'].apply(lambda x: str(x).replace('.', ''))
        disney_rug.loc[:,'weight'] = disney_rug['weight'].apply(lambda x: int(x) * 100 if int(x) < 100 else x)
        not_kg.loc[not_kg['weight'].index.isin(disney_rug.index), 'weight'] = disney_rug['weight']

        # Converting the subset from gramm to kg by diving 1000.
        not_kg.loc[:,'weight'] = not_kg['weight'].apply(lambda x: float(x) / 1000)

        # Removing the kg from the main dataset and splicing the subset back into main to finish the whole conversion.
        df.loc[:,'weight'] = df['weight'].str.replace('kg', '')
        df.loc[df['weight'].index.isin(not_kg.index), 'weight'] = not_kg['weight']

        # Converting column into float type.
        df.loc[:,'weight'] = df['weight'].astype('float')

        print('Product weigths have been sucessfully converted.')

        return df

    def clean_products_data(self, dataframe):
        '''
        The function takes a dataframe as an argument and performs the remaining cleaning of the products data dataframe.
        Removes unnecessary columns, converting all lowercase characters into uppercase characters the product code and 
        some of the rows into the final data type.

        Args:
            dataframe: the dataframe to clean
        
        Returns:
            The cleaned dataframe.
        '''

        df = dataframe

        # Removes unnecessary column
        df = df.drop(labels = ['Unnamed: 0'], axis = 1)

        # Converting all the letters in the product code column to uppercase.
        df['product_code'] = df['product_code'].apply(lambda x: x.upper())
        df['date_added'] = pd.to_datetime(df['date_added'], format = 'mixed')

        print('Products dataframe has been sucessfully cleaned.')
        
        return df
    
    def clean_orders_data(self, dataframe):
        '''
        The function takes a dataframe as an argument and performs the cleaning of the orders table dataframe.
        Removes unnecessary columns and converting all lowercase characters into uppercase characters the product code.

        Args:
            dataframe: the dataframe to clean
        
        Returns:
            The cleaned dataframe.
        '''

        df = dataframe

        # Dropping columns with over 87% missing value.
        df = df.drop(labels = ['level_0', 'index', 'first_name', 'last_name', '1'], axis = 1)

        # Capitalizing all the alphabetical characters in the product_code column.
        df['product_code'] = df['product_code'].apply(lambda x: x.upper())

        print('Data order table dataframe has been sucessfully cleaned.')

        return df
    
    def clean_sales_date(self, dataframe):
        '''
        The function takes a dataframe as an argument and performs the cleaning of the sales data dataframe.
        Removes NULL values,unnecessary rows.
        Converts some of the rows into the final data type.
        Args:
            dataframe: the dataframe to clean
        
        Returns:
            The cleaned dataframe.
        '''

        df = dataframe

        # Removing all NaNs where all the columns are null.
        mask = df[df.isnull().any(axis=1)]
        df = df[~df.index.isin(mask.index)]

        # Removing gibberish data based on the time_period.
        times_of_day = ['Evening', 'Midday', 'Morning', 'Late_Hours']
        mask2 = df[~df['time_period'].isin(times_of_day)]
        df = df[~df.index.isin(mask2.index)]

        # Converting columns into the final data type.
        df['timestamp'] = pd.to_datetime(df['timestamp'], format = '%H:%M:%S')
        df['month'] = df['month'].astype('int16')
        df['year'] = df['year'].astype('int32')
        df['day'] = df['day'].astype('int16')

        print('Sales date dataframe has been sucessfully cleaned.')

        return df
