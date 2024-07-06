# Data Modeling with Postgres & Bulding ETL Pipeline Using Python
## Introduction
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.
## Project Description
In this project, I'll apply data modeling with Postgres and build an ETL pipeline using Python. define fact and dimension tables for a star schema for a particular analytic focus, and write an ETL pipeline that transfers data from files in two local directories into these tables in Postgres using Python and SQL.

The team needs to do fast aggregations on large amounts of data. For this reason, the Star Schema was chosen to be denormalized, as this allows for fast aggregations and simplified queries. The ETL pipeline is also there to move the data into the existing data model, which justifies its use.

![Star Schema](photo/star_schema.png "Star_schema")

## File guide: 

#### 1. sql_queries.py 

This file Here constains all the SQL queries that will CREATE, INSERT, and SELECT from the Fact and Dimension tables.

#### 2. create_tables.py

This file will actually run the CREATE queries in **sql_queries.py** and create the fact and dimesion tables.

#### 3. ETL.ipynb 

Here, a test run is made on the main ETL pipeline, just to see how the data in the SQL tables are being INSERTED. This is done through using pandas to manipulate the dataframes and observe them beforehand. 

#### 4. ETL.py 

This file is the final file that runs the script on all the records, and inserting all the correct data into the fact/dimension tables. 

#### 5. test.ipynb

file that select from the created tables and confirm that the work above has been done correctly. 

