# Multinational Retail Data Centralization Project

You work for a multinational company that sells various goods across the globe.

Currently, their sales data is spread across many different data sources making it not easily accessible or analysable by current members of the team.

In an effort to become more data-driven, your organisation would like to make its sales data accessible from one centralised location.

Your first goal will be to produce a system that stores the current company data in a database so that it's accessed from one centralised location and acts as a single source of truth for sales data.

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

This is an implementation of the Hangman game, where the computer thinks of a word and the user tries to guess it.

## Milestone 1

The first milestone is the setup of the github repository.

## Milestone 2

----WIP----

The creation of the main source files including ...
The creation of the local database for the data that has been extracted from various sources.
SQL files that contains the table creation queries.

## Milestone 3

---WIP----

## Milestone 4

---WIP----

# Installation 



# How to use


# File structure

The files for the project can be found in the ... folder.

```
.
├── creds
│   ├── credentials.yaml            -------> # ignored
│   └── dummy_credentials.yaml
├── database
│   ├── create_dim_date_times.sql
│   ├── create_orders.sql
│   ├── create_products.sql
│   ├── create_store.sql
│   ├── create_user_card_table.sql
│   ├── create_user_table.sql
│   ├── db_create_db.sql
│   └── yaml_cred_extraction_test.py
├── data_files
│   ├── legacy_users.csv
│   ├── orders_table.csv
│   ├── products.csv
│   ├── sale_date.csv
│   ├── stores.csv
│   └── user_card_data.csv
├── Licence.txt
├── README.md
└── source
    ├── cred_reader.py
    ├── database_utils.py
    ├── data_cleaning.py
    ├── data_extraction.py
    └── __pycache__
        ├── cred_reader.cpython-311.pyc
        ├── database_utils.cpython-311.pyc
        └── data_cleaning.cpython-311.pyc

```

# Licence

CC0 1.0 Universal - see Licence.txt