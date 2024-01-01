from data_extraction import DataExtractor


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


    # Function call of the extraction, cleaning and database upload process.
    de.extract_first_source() 
    de.extract_second_source()
    de.extract_third_source()
    de.extract_forth_source()
    de.extract_fifth_source()
    de.extract_sixth_source()

    # Altering the local database tables and adding primary and foreign keys.
    de.local_rds_db.alter_tables_data_types()
    de.local_rds_db.alter_tables_keys()
    
    # Performing data analysis with SQL.
    de.local_rds_db.select_query()