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

    def clean_user_data(self, dataframe):
        '''
        The function cleans the legacy user dataframe. Removes NULL values, leading or trailing white spaces, special characters,
        removes unnecessary columns and rows.
        '''

        df = dataframe

        # Drop rows where all of them are NaN's
        mask = df[df.isnull().any(axis=1)]
        df = df[~df.index.isin(mask.index)]
        
        # Drop column Unnamed: 0, it's a duplicate of the index column
        df = df.drop(labels = 'index', axis = 1)


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
    
    def clean_card_data(self, dataframe):
        '''
        The function cleans the user card dataframe. Removes NULL values, special characters,
        removes unnecessary rows and imputes values where it's possible.
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
                #df['expiry_date'] = pd.to_datetime(df['expiry_date'], format = '%m/%y')    <----UNECESSARY
        df['card_number'] = df['card_number'].astype('int64')

        print('Data user_card_data dataframe has been sucessfully cleaned.')

        return df
    
    def clean_data_store(self, dataframe):
        '''
        The function cleans the stores dataframe. Removes NULL values, special characters,
        removes unnecessary rows and imputes values where necessary.
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
                #df['longitude'] = df['longitude'].astype('float32')        <----UNECESSARY
                #df['latitude'] = df['latitude'].astype('float32')          <----UNECESSARY
        df['staff_numbers'] = df['staff_numbers'].astype('int16')       
                #df['country_code'] = df['country_code'].astype('category') <----UNECESSARY
                #df['continent'] = df['continent'].astype('category')       <----UNECESSARY
                #df['store_type'] = df['store_type'].astype('category')     <----UNECESSARY

        print('Store dataframe has been sucessfully cleaned.')

        return df

    def convert_product_weights(self, dataframe):
        '''
        The function converts the weight of products into a unified weight.

        Return:
            Returns the product dataframe.
        '''

        df = dataframe

        # Removing the gibberish data from the dataframe based on the removed column for easy conversion of weights.
        list = ['Still_avaliable', 'Removed']
        mask2 = df[~df['removed'].isin(list)]
        df = df[~df.index.isin(mask2.index)]

        # Based on the 1:1 conversion rate ml simply replaced with gramm for further conversion.
        df['weight'] = df['weight'].str.replace('ml', 'g') 

        # Separating the weights that not in kg into a subset of the dataframe and removing g.
        not_kg = df[~df['weight'].str.contains('kg')]
        not_kg['weight'] = not_kg['weight'].str.replace('g','')

        # Multipack conversion to single gramm within the subset, after conversion splicing them back into the subset.
        mg = not_kg[not_kg['weight'].str.contains('x')]
        mg['weight']= mg['weight'].apply(lambda x : int(x.split(' ')[0]) * int(x.split(' ')[2]))
        not_kg.loc[not_kg['weight'].index.isin(mg.index), 'weight'] = mg['weight']
        
        # There is a product with oz unit, converting it based on the conversion rate 1oz = 28.35g.
        oz = df[df['weight'].str.contains('oz')]
        oz['weight'] = oz['weight'].str.replace('oz', '')
        oz['weight'] = oz['weight'].apply(lambda x: int(x) * 28.35)
        not_kg.loc[not_kg['weight'].index.isin(oz.index), 'weight'] = oz['weight']

        # Separating Goodmans products as the weight is not correct. Fixing the splicing them back into the subset.
        goodmans = not_kg[not_kg['product_name'].str.contains('Goodmans')]
        goodmans['weight'] = goodmans['weight'].str.lstrip('0.')
        goodmans['weight'] = goodmans['weight'].apply(lambda x: str(x).replace('.','') if len(x) > 4 else x)
        goodmans['weight'] = goodmans['weight'].apply(lambda x: str(x).replace('.5','500'))
        not_kg.loc[not_kg['weight'].index.isin(goodmans.index), 'weight'] = goodmans['weight']

        # Correcting the only value with a weight enging (<whitespace>.)
        not_kg['weight'] = not_kg['weight'].apply(lambda x: str(x).replace(' .', ''))

        # Separating the Disney and Rug products and fixing the weight where it needs to be then splicing them back into the subset.
        disney_rug = not_kg.loc[not_kg['product_name'].str.contains(r'^(?=.*Disney)|(?=.*Rug)')]
        disney_rug['weight'] = disney_rug['weight'].apply(lambda x: str(x).replace('.', ''))
        disney_rug['weight'] = disney_rug['weight'].apply(lambda x: int(x) * 100 if int(x) < 100 else x)
        not_kg.loc[not_kg['weight'].index.isin(disney_rug.index), 'weight'] = disney_rug['weight']

        # Converting the subset from gramm to kg by diving 1000.
        not_kg['weight'] = not_kg['weight'].apply(lambda x: float(x) / 1000)

        # Removing the kg from the main dataset and splicing the subset back into main to finish the whole conversion.
        df['weight'] = df['weight'].str.replace('kg', '')
        df.loc[df['weight'].index.isin(not_kg.index), 'weight'] = not_kg['weight']

        # Converting column into float type.
        df['weight'] = df['weight'].astype('float')

        print('Product weigths have been sucessfully converted.')

        return df

    def clean_products_data(self, dataframe):
        '''
        The function cleans the products dataframe. Removes NULL values, special characters,
        removes unnecessary rows and imputes values where necessary.
        '''

        df = dataframe

        # Removes unecessary column
        df = df.drop(labels = ['Unnamed: 0'], axis = 1)

        # Removing the pound sign from the product price column and renaming the column to reflect that all units are in pound(£).
        # Also renaming the weight ccolumn to reflect that all units are in kg.
        #df['product_price'] = df['product_price'].str.replace('£','')
        #df.rename(columns = {'product_price':'product_price_(£)', 'weight':'weight(kg)'})

        # Converting all the letters in the product code column to uppercase.
        df['product_code'] = df['product_code'].apply(lambda x: x.upper())

                #df['product_price'] = df['product_price'].astype('float')  <----UNECESSARY
                #df['category'] = df['category'].astype('category')         <----UNECESSARY
        df['date_added'] = pd.to_datetime(df['date_added'], format = 'mixed')
                #df['removed'] = df['removed'].astype('category')           <----UNECESSARY

        print('Products dataframe has been sucessfully cleaned.')
        
        return df
    
    def clean_orders_data(self, dataframe):

        df = dataframe

        # Dropping columns with over 87% missing value.
        df = df.drop(labels = ['level_0', 'index', 'first_name', 'last_name', '1'], axis = 1)

        # Capitalizing all the alphabetical characters in the product_code column.
        df['product_code'] = df['product_code'].apply(lambda x: x.upper())

        print('Data order table dataframe has been sucessfully cleaned.')

        return df
    
    def clean_sales_date(self, dataframe):

        df = dataframe

        # Removing all NaNs where all the columns are null.
        mask = df[df.isnull().any(axis=1)]
        df = df[~df.index.isin(mask.index)]

        # Removing gibberish data based on the time_period.
        months = ['Evening', 'Midday', 'Morning', 'Late_Hours']
        mask2 = df[~df['time_period'].isin(months)]
        df = df[~df.index.isin(mask2.index)]

        # Converting columns into the final data type.
        df['timestamp'] = pd.to_datetime(df['timestamp'], format = '%H:%M:%S')
                #df['time_period'] = df['time_period'].astype('category')   <----UNECESSARY
        df['month'] = df['month'].astype('int16')
        df['year'] = df['year'].astype('int32')
        df['day'] = df['day'].astype('int16')

        print('Sales date dataframe has been sucessfully cleaned.')

        return df