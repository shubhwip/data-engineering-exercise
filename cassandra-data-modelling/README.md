# Cassandra-Data-Modelling

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app.

They'd like a data engineer to create an Apache Cassandra database which can create queries on song play data to answer the questions, and wish to bring you on the project. Your role is to create a database for this analysis. You'll be able to test your database by running queries given to you by the analytics team from Sparkify to create the results.

## Project Overview

In this project, I'll apply what I've learned on data modeling with Apache Cassandra and complete an ETL pipeline using Python. To complete the project, I will need to model your data by creating tables in Apache Cassandra to run queries. I am provided with part of the ETL pipeline that transfers data from a set of CSV files within a directory to create a streamlined CSV file to model and insert data into Apache Cassandra tables.

Udacity has provided me with a project template that takes care of all the imports and provides a structure for ETL pipeline I'd need to process this data.

## Datasets Used

For this project, I'll be working with one dataset: `event_data`. The directory of CSV files partitioned by date. Here are examples of filepaths to two files in the dataset:

``` sh
event_data/2018-11-08-events.csv
event_data/2018-11-09-events.csv
```

## Design Queries

- Give me the artist, song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession = 4
- Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
- Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'

## Prerequisites

- Python3
- Python Modules like configparser, pandas, psycopg2
- Cassandra

## Scripts Used

- `cassandra-data-modelling.ipynb` python notebook will be used to run all ETL tasks and subsequently executing required queries.

## Steps followed

- Creating list of filepaths to process original event csv data files
![alt text](./images/image_event_datafile_new.jpg "Event data CSV post processing")

- Created Keyspace If not exists with name `udacity`

### Design Queries

- Since it is best practice to have one table per query thus for following three queries I created three tables


#### Query 1

Give me the artist, song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession = 4

- Dropped the table artist_song_history if exists
- Created the table artist_song_history if exists with following structure

``` sh
sessionId int,
itemInSession int,
artist varchar,  
song varchar, 
length float,  
PRIMARY KEY (sessionId, itemInSession)
```

- Chosen primary keys as sessionId, itemInSession to uniquely identify row
- Using CSVReader, inserted relevant data into artist_song_history table
- Executed Select Operation to verify the results

``` sh
SELECT artist, song, length FROM artist_song_history WHERE sessionId = 338 and itemInSession = 4
Output :  
Faithless Music Matters (Mark Knight Dub) 495.3073
```

#### Query 2

Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182

- Dropped the table user_and_music_history if exists
- Created the table user_and_music_history if exists with following structure

``` sh
userId int, 
sessionId int,
itemInSession int, 
artist varchar,  
song varchar, 
firstname varchar, 
lastname varchar, 
PRIMARY KEY ((userId, sessionId), itemInSession)
```

- Chosen primary keys as (userId, sessionId) and itemInSession as composite key to uniquely identify row
- Using CSVReader, inserted relevant data into user_and_music_history table
- Executed Select Operation to verify the results

``` sh
SELECT artist, song, firstname, lastname FROM user_and_music_history WHERE userId = 10 and sessionId = 182
Output : 
Down To The Bone Sylvie Cruz
Three Drives Sylvie Cruz
Sebastien Tellier Sylvie Cruz
Lonnie Gordon Sylvie Cruz
```

#### Query 3

Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'

- Dropped the table user_history_with_songs if exists
- Created the table user_history_with_songs if exists with following structure

``` sh
userId int,
song varchar,
firstname varchar, 
lastname varchar, 
PRIMARY KEY (userId, song)
```

- Chosen primary key as song to uniquely identify row
- Using CSVReader, inserted relevant data into user_history_with_songs table
- Executed Select Operation to verify the results

``` sh
SELECT firstname, lastname FROM user_history_with_songs WHERE song = 'All Hands Against His Own'
Output : 
Jacqueline Lynch
Sara Johnson
Tegan Levine
```

## References

- <https://review.udacity.com/#!/rubrics/2475/view>
- <https://stackoverflow.com/questions/24949676/difference-between-partition-key-composite-key-and-clustering-key-in-cassandra/24953331#24953331>
- <https://stackoverflow.com/questions/18168379/cassandra-choosing-a-partition-key>
- Cassandra Data Types - <https://docs.datastax.com/en/archived/cql/3.3/cql/cql_reference/cql_data_types_c.html>

## Review Considerations

### ETL Pipeline Processing

- Student creates event_data_new.csv file.
- Student uses the appropriate datatype within the CREATE statement.

``` sh
✅ Good job! You chose the correct data type for each column.
Be sure to keep this link for future reference in case you want to check the available Cassandra data types.
```

### Data Modeling

- Student creates the correct Apache Cassandra tables for each of the three queries. The CREATE TABLE statement should include the appropriate table.
- Student demonstrates good understanding of data modeling by generating correct SELECT statements to generate the result being asked for in the question.
The SELECT statement should NOT use ALLOW FILTERING to generate the results.
- Student should use table names that reflect the query and the result it will generate. Table names should include alphanumeric characters and underscores, and table names must start with a letter.
- The sequence in which columns appear should reflect how the data is partitioned and the order of the data within the partitions.

For example, your table should be like the following:

``` sh
CREATE TABLE IF NOT EXISTS music_library (
      column_C INT, 
      cloumn_B INT,
      column_A TEXT,  
      column_D TEXT,
     PRIMARY KEY (column_C, column_B)
)
INSERT INTO music_library (column_C, column_B, column_A, column_D)
```

### PRIMARY KEYS

- The combination of the PARTITION KEY alone or with the addition of CLUSTERING COLUMNS should be used appropriately to uniquely identify each row.

### Presentation

- The notebooks should include a description of the query the data is modeled after.

``` sh
✅ Really good work adding a header for each table explaining how you modeled them!
Be sure to update it once you make the suggested change above to the third table
```

- Code should be organized well into the different queries. Any in-line comments that were clearly part of the project instructions should be removed so the notebook provides a professional look.

``` sh
✅ Great job here! The notebook is clean and organized. You've removed unnecessary comments and instructions.
Even though we're working with a Jupyter notebook, it's very common to share them with coworkers or present your results to someone.
```

- You can add description of your PRIMARY KEY and how you arrived at the decision to use each for the query
- Use Panda dataframes to add columns to your query output
- Improvement - we can insert data while doing csv parsing once for all tables

