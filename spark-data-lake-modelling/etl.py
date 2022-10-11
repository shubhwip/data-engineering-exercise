import configparser
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date, LongType as Long
import pyspark.sql.functions as f

# constants
column_expression = "{} as {}"
parquet_output_data_location = "{output_path}/{sub_output_location}"

# current directory
current_directory = os.path.dirname(os.path.abspath(__file__))
# path to data lake configuration
data_lake_configuration = os.path.join(current_directory, 'dl.cfg')

config = configparser.ConfigParser()
# reading data lake configuration
config.read(data_lake_configuration)
    
# Loading AWS Secrets
os.environ['AWS_ACCESS_KEY_ID'] = config.get("AWS", "AWS_ACCESS_KEY_ID")
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get("AWS", "AWS_SECRET_ACCESS_KEY")
# location of input data
input_data = config.get("S3", "INPUT_DATA_LOCATION")
# location of output data
output_data = config.get("S3", "OUTPUT_DATA_LOCATION")
# Packages to load as per data source
spark_packages = config.get("SPARK", "PACKAGES")

def create_spark_session():
    """
    Creates spark session with required packages as per the cloud provider or local environment
    """
    spark = SparkSession.builder \
        .config("spark.jars.packages", "{spark_packages}".format(spark_packages=spark_packages)) \
        .getOrCreate()
    return spark

@udf
def parse_timestamp(ts):
    """
    parses the timestamp from milliseconds to seconds
    """
    from datetime import datetime
    date_string = str(datetime.fromtimestamp(ts/1000))
    return date_string

def process_song_data(spark, input_data, output_data):
    """
    processes song data into spark data frames
    creates new dataframes for songs and artists table
    persist this data to parquet file structure
    """
    
    song_data_location = input_data + "/song_data/*/*/*/*.json"

    # song schema
    song_schema = R([
        Fld("artist_id",Str()),
        Fld("artist_latitude",Dbl()),
        Fld("artist_location",Str()),
        Fld("artist_longitude",Dbl()),
        Fld("artist_name",Str()),
        Fld("duration",Dbl()),
        Fld("num_songs",Int()),
        Fld("song_id",Str()),
        Fld("title",Str()),
        Fld("year",Int()),
    ])
    # read song data file
    song_data = spark.read.option("recursiveFileLookup","true") \
                .json(song_data_location, schema=song_schema)

    logging.info("Songs Schema")
    song_data.printSchema()
    
    # extract columns to create songs table
    songs_table = song_data.select("song_id", "title", "artist_id", "year", "artist_name", "duration") \
                    .dropDuplicates(["song_id"])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_name") \
        .mode("overwrite") \
        .parquet(parquet_output_data_location.format(output_path=output_data, sub_output_location="songs.parquet"))

    # extract columns to create artists table
    artists_table_fields = ["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]
    artists_table_new_fields = ["artist_id", "name", "location", "latitude", "longitude"]
    artists_table_exprs = [ column_expression.format(oldField, newField) for (oldField, newField) in zip(artists_table_fields, artists_table_new_fields) ]
    
    artists_table = song_data.selectExpr(*artists_table_exprs).dropDuplicates(["artist_id"])
    
    # write artists table to parquet files
    artists_table.write.partitionBy("location") \
        .mode("overwrite") \
        .parquet(parquet_output_data_location.format(output_path=output_data, sub_output_location="artists.parquet"))

    return song_data


def process_log_data(spark, input_data, output_data, songs_data):
    """
    Processes event data into spark data frame
    Creates user, time and songplays table using spark data frame
    Persist data to parquet files
    """

    event_data_location = input_data + "/log_data/*.json"

    # event schema
    event_schema = R([
        Fld("artist",Str()),
        Fld("auth",Str()),
        Fld("firstName",Str()),
        Fld("gender",Str()),
        Fld("itemInSession",Int()),
        Fld("lastName",Str()),
        Fld("length",Dbl()),
        Fld("level",Str()),
        Fld("location",Str()),
        Fld("method",Str()),
        Fld("page",Str()),
        Fld("registration",Dbl()),
        Fld("sessionId",Int()),
        Fld("song",Str()),
        Fld("status",Str()),
        Fld("ts",Long()),
        Fld("userAgent",Str()),
        Fld("userId",Str())
    ])

    # read log data file
    events_data = spark.read.json(event_data_location, schema=event_schema)

    # filter by actions for song plays
    events_data = events_data.filter(events_data.page == "NextSong")

    # create timestamp column from original timestamp column
    events_data = events_data.withColumn("ts", parse_timestamp("ts"))

    # cast userId column to integer
    events_data = events_data.withColumn("userId", events_data["userId"].cast(Int()))
    
    logging.info("Events Schema")
    events_data.printSchema()

    # extract columns for users table    
    users_table_fields = ["userId", "firstName","lastName", "gender", "level"]
    users_table_new_fields = ["user_id", "first_name","last_name", "gender", "level"]
    users_table_exprs = [ column_expression.format(oldField, newField) for (oldField, newField) in zip(users_table_fields, users_table_new_fields) ]
    users_table_with_drop_duplicates = events_data.selectExpr(*users_table_exprs)
    users_table = users_table_with_drop_duplicates.dropDuplicates(["user_id"])

    # write users table to parquet files
    users_table.write.partitionBy("gender", "level") \
        .mode("overwrite") \
            .parquet(parquet_output_data_location.format(output_path=output_data, sub_output_location="users.parquet"))

    # extract columns to create time table
    time_table_fields = ["ts"]
    time_table_new_fields = ["start_time"]
    time_table_exprs = [ column_expression.format(oldField, newField) for (oldField, newField) in zip(time_table_fields, time_table_new_fields) ]
    time_table = events_data.selectExpr(*time_table_exprs).dropDuplicates(["start_time"])
    
    time_table = time_table.withColumn("hour", f.hour("start_time"))
    time_table = time_table.withColumn("day", f.dayofweek("start_time"))
    time_table = time_table.withColumn("week", f.weekofyear("start_time"))
    time_table = time_table.withColumn("month", f.month("start_time"))
    time_table = time_table.withColumn("year", f.year("start_time"))
    # Valid parantheses enclosing for logical operators is necessary
    time_table = time_table.withColumn("weekday", ((f.dayofweek("start_time") > 0) & (f.dayofweek("start_time") < 6)) )
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").mode("overwrite").parquet(parquet_output_data_location.format(output_path=output_data, sub_output_location="time.parquet"))

    # condition for joing events and songs spark dataframes
    condition = (( events_data["artist"] == songs_data["artist_name"]) \
                    & (events_data["song"] == songs_data["title"]) \
                    & (events_data["length"] == songs_data["duration"]) )

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = events_data. \
        join(songs_data, condition, "left_outer") \
            .select(events_data["ts"], \
                    events_data["userId"], \
                    events_data["level"], \
                    songs_data["song_id"], \
                    songs_data["artist_id"], \
                    events_data["sessionId"], \
                    events_data["location"], \
                    events_data["userAgent"] \
            )
    songplays_table = songplays_table.withColumnRenamed("ts", "start_time")\
                            .withColumnRenamed("userId", "user_id") \
                            .withColumnRenamed("sessionId", "session_id") \
                            .withColumnRenamed("userAgent", "user_agent")
    
    songplays_table = songplays_table.withColumn("month", f.month("start_time"))
    songplays_table = songplays_table.withColumn("year", f.year("start_time"))
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month") \
        .mode("overwrite") \
            .parquet(parquet_output_data_location.format(output_path=output_data, sub_output_location="songplays.parquet"))


def main():
    spark = create_spark_session()
    song_data = process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data, song_data)

if __name__ == "__main__":
    main()
