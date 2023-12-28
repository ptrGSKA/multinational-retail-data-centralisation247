# Multinational Retail Data Centralization Project

This work for a multinational company that sells various goods across the globe.

Currently, their sales data is spread across many different data sources making it not easily accessible or analysable by current members of the team.

In an effort to become more data-driven, the organisation would like to make its sales data accessible from one centralised location.

The first goal will be to produce a system that stores the current company data in a database so that it's accessed from one centralised location and acts as a single source of truth for sales data.

# Table of Contents
1. [Description](#description)
    - [Milestone 1](#milestone-1)
    - [Milestone 2](#milestone-2)
    - [Milestone 3](#milestone-3)
    - [Milestone 4](#milestone-4)
2. [Installation](#installation)
3. [How to use](#how-to-use)
4. [File structure](#file-structure)
5. [Licence](#licence)

# Description

The project is to develop a system - data pipeline - that extracts information from various sources including API requests, AWS S3 and AWS database. After the extraction the the data needs to be cleaned and uploded to a centralized database - in this case a local postgresql database - where later analysis can be performed on the data to gain valuable insights and make recommendations.

The code first inspects the local database, searching the existence of the database and creates it if doesn't exist and also creates the required tables for the data. If the database exists at the first place it still performs a check on the existence of the tables and creates the ones that are not present. After this the code starts to request information from a remote source to query about the database. Upon confirming the required table to extract it performs the extraction, saving and cleaning of the data and then it uploads it to the database.

The extraction of further data continues with extracting information from a pdf file, downloading data via an API request, AWS S3 bucket, AWS dtabase again and finally it performs an other API request to download clean and to upload the data into the local database.

## Milestone 1

The first milestone is the setup of the github repository.

## Milestone 2

The construction of the core code files including database communication related, data cleaning, data extraction and credential reader python files.
This section of the project also encompassed the establishment of the local database and the creation of relevant tables to store the project's data that has been extracted from various sources.
The created tables will be altered later to for the correct data type.

## Milestone 3

Setting up the star-based databse schema by altering the table columns data type. Cleaning the data where necessar and adding new columns based on existing data.
This section also contains the addition of Primary and Foreign Keys to tables.


## Milestone 4

---WIP----

# Installation 



# How to use


# File structure

The files for the project can be found in the ... folder.

```
.
├── creds
│   ├── credentials.yaml
│   └── dummy_credentials.yaml
├── database
│   ├── alter_all_tables_foreign_key.sql
│   ├── alter_all_tables_primary_keys.sql
│   ├── alter_dim_card_details.sql
│   ├── alter_dim_date_times.sql
│   ├── alter_dim_products.sql
│   ├── alter_dim_store_details.sql
│   ├── alter_dim_users_table.sql
│   ├── alter_orders_table.sql
│   ├── create_db.sql
│   ├── create_dim_card_details.sql
│   ├── create_dim_date_times.sql
│   ├── create_dim_products.sql
│   ├── create_dim_store_details.sql
│   ├── create_dim_users.sql
│   └── create_orders_table.sql
├── data_files
│   ├── legacy_users.csv
│   ├── orders_table.csv
│   ├── products.csv
│   ├── sale_date.csv
│   ├── stores.csv
│   └── user_card_data.csv
├── Licence.txt
├── README.md
└── source
    ├── cred_reader.py
    ├── database_utils.py
    ├── data_cleaning.py
    ├── data_extraction.py
    ├── __pycache__
    │   ├── cred_reader.cpython-311.pyc
    │   ├── database_utils.cpython-311.pyc
    │   ├── data_cleaning.cpython-311.pyc
    │   └── data_extraction.cpython-311.pyc
    └── start_data_pipeline.py
```

# Licence

CC0 1.0 Universal - see Licence.txt