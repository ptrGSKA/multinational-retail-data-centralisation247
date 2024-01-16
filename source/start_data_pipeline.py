from data_extraction import DataExtractor
from decorator_class import DecoratorClass


if __name__ == '__main__':

    de = DataExtractor()
    
    if de.local_rds_db.check_db_existence():
        print('Checking for existence of tables. \n')

        table_missmatch = de.local_rds_db.check_db_tables()
        if len(table_missmatch) > 0:
            de.local_rds_db.create_tables(table_missmatch)
            print('All tables are present in the local database - DB preparation has been finished\n')
        else:
            print('All tables are present in the local database - DB preparation has been finished')
    else:
        de.local_rds_db.create_database()
        print('Checking for existence of tables. \n')

        table_missmatch = de.local_rds_db.check_db_tables()
        if len(table_missmatch) > 0:
            last_table_check = de.local_rds_db.create_tables(table_missmatch)

            if len(last_table_check) == 0:
                print('All tables are present in the local database - DB preparation has been finished\n')
        else:
            print('All tables are present in the local database - DB preparation has been finished')

    @DecoratorClass
    def extract_first_source():
        print('The availables tables are:')
        for table in de.tables:
            print(f'-------> {table}')
        print(f'Table {de.tables[1]} is being extracted.\n')

        user_df = de.read_rds_table(de.remote_rds_db.engine, de.tables[1])
        clean_user_df = de.cleaning.clean_user_data(user_df)
        de.local_rds_db.upload_to_db(clean_user_df, 'dim_users')

    @DecoratorClass
    def extract_second_source():
        card_df = de.retrieve_pdf_data(de.pdf_data)
        clean_card_df = de.cleaning.clean_card_data(card_df)
        de.local_rds_db.upload_to_db(clean_card_df, 'dim_card_details')

    @DecoratorClass
    def extract_third_source():
        stores = de.list_number_of_stores(de.num_of_stores, de.creds)
        store_df = de.retrieve_stores_data(de.store_endpoint, stores, de.creds)
        clean_store_df = de.cleaning.clean_data_store(store_df)
        de.local_rds_db.upload_to_db(clean_store_df, 'dim_store_details')

    @DecoratorClass
    def extract_forth_source():
        products_df = de.extract_from_s3(de.s3_address)
        products_conversion = de.cleaning.convert_product_weights(products_df)
        clean_products_df = de.cleaning.clean_products_data(products_conversion)
        de.local_rds_db.upload_to_db(clean_products_df, 'dim_products')

    @DecoratorClass
    def extract_fifth_source():
        print(f'Table {de.tables[2]} is being extracted.')
        orders_df = de.read_rds_table(de.remote_rds_db.engine, de.tables[2])
        clean_orders_df = de.cleaning.clean_orders_data(orders_df)
        de.local_rds_db.upload_to_db(clean_orders_df, 'orders_table')

    @DecoratorClass
    def extract_sixth_source():
        sales_date_df = de.extract_json_data(de.json_address)
        clean_sales_date_df = de.cleaning.clean_sales_date(sales_date_df)
        de.local_rds_db.upload_to_db(clean_sales_date_df, 'dim_date_times')


    # Function call of the extraction, cleaning and database upload process.
    extract_first_source() 
    extract_second_source()
    extract_third_source()
    extract_forth_source()
    extract_fifth_source()
    extract_sixth_source()

    # Altering the local database tables and adding primary and foreign keys.
    de.local_rds_db.alter_tables_data_types()
    de.local_rds_db.alter_tables_keys()
    
    # Performing data analysis with SQL. query 9  should contain -  SET intervalstyle = 'postgres_verbose';
    de.local_rds_db.select_query()