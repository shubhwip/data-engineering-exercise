# Postgres-Data-Modelling

## Prerequisites
- Python3
- Python Modules like configparser, pandas, psycopg2

## Description
#### create_tables.py
The script, create_tables.py successfully connects to the Sparkify database, drops any tables if they exist, and creates the tables.

#### sql_queries.py
CREATE statements in sql_queries.py specify all columns for each of the five tables with the right data types and conditions.



## Structure

## Run Scripts
#### Run `create_tables.py`
```
  $ python3 create_tables.py
```
#### Insert Dummy Data `insert_dummy_data.py`
```
  $ python3 insert_dummy_data.py
```
#### Run ETL Script `etl.py`
```
  $ python3 etl.py
```
## References
- Udacity Rubric Guide <https://review.udacity.com/#!/rubrics/4792/view>
- Helpful Links
  - https://stackoverflow.com/questions/47541579/how-to-have-postgres-ignore-inserts-with-a-duplicate-key-but-keep-going
  
## Nice to Haves
- Insert data using the COPY command to bulk insert log files instead of using INSERT on one row at a time
- Add data quality checks
- Create a dashboard for analytic queries on your new database