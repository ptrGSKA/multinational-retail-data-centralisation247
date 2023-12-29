import data_extraction as dex
import time
import warnings

warnings.filterwarnings('ignore') 


#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-
#-TEST - TEST - TEST - TEST - TEST - TEST - TEST - TEST - TEST - TEST - TEST - TEST - TEST - TEST - TEST - TEST - TEST - TEST - TEST -#-
#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-
#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-CALLING OF THE DATA PIPELINE-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-
#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-

if __name__ == '__main__':

    de = dex.DataExtractor()
    
    if de.local_rds_db.check_db_existence():
        print('Checking for existence of tables. \n')

        table_missmatch = de.local_rds_db.check_db_tables()
        if len(table_missmatch) > 0:
            last_table_check = de.local_rds_db.create_tables(table_missmatch)

            if len(last_table_check) == 0:
                print('All tables are present in the local database - DB preparation has been finished')
        else:
            print('All tables are present in the local database - DB preparation has been finished')
    else:
        de.local_rds_db.create_database()

        table_missmatch = de.local_rds_db.check_db_tables()
        if len(table_missmatch) > 0:
            last_table_check = de.local_rds_db.create_tables(table_missmatch)

            if len(last_table_check) == 0:
                print('All tables are present in the local database - DB preparation has been finished')

    #print('Extracting data is starting in: ')
    
    #for i in range(5,0,-1):
    #    print(f'Download starts in - {i}', end = '\r')
    #    time.sleep(1)

    print('\n')

    db_instance = de.remote_rds_db.engine
    print('The availables tables are:\n')
    for table in de.tables:
        print(f'-------> {table}')
                    #table_id = int(input('Choose a table to extract the data from: ')) <--------- NO CHOICE ANYMORE
    table_name = de.tables[1]
    print(f'Table {table_name} is being extracted.')

    # First source data extraction, cleaning and uploading to local database
    user_df = de.read_rds_table(db_instance, table_name)
    clean_user_df = de.cleaning.clean_user_data(user_df)
    de.local_rds_db.upload_to_db(clean_user_df, 'dim_users')

    # Second source data extraction, cleaning and uploading to local database
    card_df = de.retrieve_pdf_data(de.pdf_data)
    clean_card_df = de.cleaning.clean_card_data(card_df)
    de.local_rds_db.upload_to_db(clean_card_df, 'dim_card_details')

    # Third source data extraction, cleaning and uploading to local database
    crds = de.read_cred()
    stores = de.list_number_of_stores(de.num_of_stores, crds)
    store_df = de.retrieve_stores_data(de.store_endpoint, stores, crds)
    clean_store_df = de.cleaning.clean_data_store(store_df)
    de.local_rds_db.upload_to_db(clean_store_df, 'dim_store_details')

    # Fourth source data extraction, cleaning and uploading to local database 
    products_df = de.extract_from_s3(de.s3_address)
    products_conversion = de.cleaning.convert_product_weights(products_df)
    clean_products_df = de.cleaning.clean_products_data(products_conversion)
    de.local_rds_db.upload_to_db(clean_products_df, 'dim_products')

    # Fifth source data extraction, cleaning and uploading to local database             <--------- NO NEED
                    #print('The avaialble tables in the database are: ', de.tables)
                    #table_id = int(input('Choose a table to extract the data from: '))  <--------- NO CHOICE ANYMORE
    table_name = de.tables[2]
    print(f'Table {table_name} is being extracted.')
    orders_df = de.read_rds_table(db_instance, table_name)
    clean_orders_df = de.cleaning.clean_orders_data(orders_df)
    de.local_rds_db.upload_to_db(clean_orders_df, 'orders_table')

    # Sixth source data extraction, cleaning and uploading to local database
    sales_date_df = de.extract_json_data(de.json_address)
    clean_sales_date_df = de.cleaning.clean_sales_date(sales_date_df)
    de.local_rds_db.upload_to_db(clean_sales_date_df, 'dim_date_times')

    # Altering the local database tables and adding primary and foreign keys.
    de.local_rds_db.alter_tables_data_types()
    de.local_rds_db.alter_tables_keys()
    
    de.local_rds_db.select_query()