# Postgres-Data-Modelling

## Prerequisites
- Python3
- Python Modules like configparser, pandas, psycopg2

## Description
#### create_tables.py
The script, create_tables.py successfully connects to the Sparkify database, drops any tables if they exist, and creates the tables.

#### sql_queries.py
CREATE/INSERT/DROP statements in sql_queries.py specify all columns for each of the five tables with the right data types and conditions.

#### etl.py
The script, etl.py connects to the Sparkify database, extracts and processes the log_data and song_data, and loads data into the five tables using INSERT SQL Operation.

#### bulketl.py
The script, etl.py connects to the Sparkify database, extracts and processes the log_data and song_data, and loads data into the five tables using COPY SQL Operation.

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
#### Run ETL Script with Copy Command `bulketl.py`
```
  $ python3 bulketl.py
```

## Output
- Songs Table contains 71 rows
- Artists Table contains 69 rows
- Time Table contains 6813 rows
- Users Table contains 96 rows
- SongPlays Table contains 1 row

## References
- Udacity Rubric Guide <https://review.udacity.com/#!/rubrics/4792/view>
- Helpful Links
  - https://stackoverflow.com/questions/47541579/how-to-have-postgres-ignore-inserts-with-a-duplicate-key-but-keep-going