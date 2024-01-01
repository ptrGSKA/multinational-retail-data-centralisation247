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

The script then reads the content of the database folder where raw queries are stored for the project and executes them on the database, altering the data types and adding primary and foreign keys. When this is done, the execution of the queries performed to answer business questions that stored in a csv file and also printed to the terminal.

## Milestone 1

The first milestone is the setup of the github repository.

## Milestone 2

The construction of the core code files including database communication related, data cleaning, data extraction and credential reader python files.
This section of the project also encompassed the establishment of the local database and the creation of relevant tables to store the project's data that has been extracted from various sources.
The created tables will be altered later to for the correct data type.

## Milestone 3

Setting up the star-based databse schema, altering the table columns data type, cleaning the data where necessary and adding new columns based on existing data.
This section also contains the addition of Primary and Foreign Keys to tables.


## Milestone 4

Utilizing SQL queries to extract information from the data and answer business questions. By performing SQL queries on the data we answer the following questions:

- How many stores the business have and in which countries?
- Which locations currently have the most stores?
- Which months produced the largest amount of sales?
- How many sales are coming from online?
- What percentage of sales come through each type of store?
- Which month in each year produced the highest cost of sales?
- What is out staff headcount?
- Which German store type is selling the most?
- How quickly is the company making sales?

# Installation 

Follow the steps to start the pipeline process:

1. Clone the github repository by typig in the Command Line Interface git clone [git_repository_url] on either operating system.
2. Navigate to the folder where the repository was cloned and in the source folder open a terminal/shell or alternatively navigate to the folder using the shell.
3. Type the command python3 start_data_pipeline.py.



# How to use

It will automatically download, clean and upload data to the database and executes the queries that is saved locally that is ready for analysis.

# File structure

The files for the project can be found in the multinational-retail-data-centralisation247 folder.

```
.
├── creds
│   ├── credentials.yaml
│   └── dummy_credentials.yaml
├── database
│   ├── alter_tables
│   │   ├── alter_dim_card_details.sql
│   │   ├── alter_dim_date_times.sql
│   │   ├── alter_dim_products.sql
│   │   ├── alter_dim_store_details.sql
│   │   ├── alter_dim_users.sql
│   │   ├── alter_orders_table.sql
│   │   ├── alter_tables_foreign_key.sql
│   │   └── alter_tables_primary_key.sql
│   ├── create_tables
│   │   ├── create_db.sql
│   │   ├── create_dim_card_details.sql
│   │   ├── create_dim_date_times.sql
│   │   ├── create_dim_products.sql
│   │   ├── create_dim_store_details.sql
│   │   ├── create_dim_users.sql
│   │   └── create_orders_table.sql
│   ├── query_results
│   │   ├── select_query_1.csv
│   │   ├── select_query_2.csv
│   │   ├── select_query_3.csv
│   │   ├── select_query_4.csv
│   │   ├── select_query_5.csv
│   │   ├── select_query_6.csv
│   │   ├── select_query_7.csv
│   │   ├── select_query_8.csv
│   │   └── select_query_9.csv
│   └── select_query
│       ├── select_query_1.sql
│       ├── select_query_2.sql
│       ├── select_query_3.sql
│       ├── select_query_4.sql
│       ├── select_query_5.sql
│       ├── select_query_6.sql
│       ├── select_query_7.sql
│       ├── select_query_8.sql
│       └── select_query_9.sql
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
    ├── decorator_class.py
    ├── __pycache__
    │   ├── cred_reader.cpython-311.pyc
    │   ├── database_utils.cpython-311.pyc
    │   ├── data_cleaning.cpython-311.pyc
    │   ├── data_extraction.cpython-311.pyc
    │   ├── decorator_class.cpython-311.pyc
    │   └── extractors.cpython-311.pyc
    └── start_data_pipeline.py
```

# Licence

CC0 1.0 Universal - see Licence.txt