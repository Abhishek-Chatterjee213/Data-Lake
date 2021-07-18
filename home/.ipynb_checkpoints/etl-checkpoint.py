import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Dat, TimestampType


# Read aws creds from config file
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['aws']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['aws']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    
    '''
        Creates a spark session
        Parameters : None
        Returns : SparkSession object
    '''
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    
    '''
        Processes songs_data divided by tracks and loads into different dimenssion tables -- 
            1. songs
            2. artists
            
        Parameters : 
            1. spark - SparkSession object
            2. input_data - Source S3 bucket to read the data
            3. output_data - Destination S3 bucket for writting data
            
        Returns : None
    '''
    
    # get filepath to song data file
    
    song_data_path =  input_data + 'song_data/*/*/*/*.json'
    
    song_schema = R([
        Fld("artist_id",Str()),
        Fld("artist_latitude",Dbl()),
        Fld("artist_location",Str()),
        Fld("artist_longitude",Dbl()),
        Fld("artist_name",Str()),
        Fld("duration",Dbl()),
        Fld("num_songs",Int()),
        Fld("title",Str()),
        Fld("year",Int()),
    ])
    
    # read song data file
    df = spark.read.json(song_data_path, schema=song_schema)

    # extract columns to create songs table
    songs_table = df.select(['title','artist_id','year','duration'])
    
    # set song_id to auto increment value
    songs_table = songs_table.withColumn('song_id', monotonically_increasing_id())
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy("year", "artist_id").parquet(output_data + "songs/")

    # extract columns to create artists table
    artist_columns = ['artist_id as artist_id','artist_name as name','artist_location as location','artist_longitude as longitude','artist_latitude as latitude']
    artists_table = df.selectExpr(*artist_columns).dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + "artists/")


def process_log_data(spark, input_data, output_data):
    
    '''
        Processes log_data divided by dates & hours and loads into different dimenssion tables and a fact table-- 
            Fact Tables --
                1. users
                2. time
                
            Dimenssion Table -- songplays
            
        Parameters : 
            1. spark - SparkSession object
            2. input_data - Source S3 bucket to read the data
            3. output_data - Destination S3 bucket for writting data
            
        Returns : None
    '''
    
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/*/*/*.json')

    # read log data file
    log_df = spark.read.json(log_data)

    # filter by actions for song plays
    log_df = log_df.filter(log_df.page == 'NextSong')

    # extract columns for users table
    users_fields = ["userId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level"]
    users_table = log_df.selectExpr(*users_fields).dropDuplicates()

    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data + 'users')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: x / 1000, TimestampType())
    log_df = log_df.withColumn("timestamp", get_timestamp(log_df.ts))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x), TimestampType())
    log_df = log_df.withColumn("start_time", get_datetime(log_df.timestamp))

    # extract columns to create time table
    log_df = log_df.withColumn("hour", hour("start_time")) \
        .withColumn("day", dayofmonth("start_time")) \
        .withColumn("week", weekofyear("start_time")) \
        .withColumn("month", month("start_time")) \
        .withColumn("year", year("start_time")) \
        .withColumn("weekday", dayofweek("start_time"))

    time_table = log_df.select("start_time", "hour", "day", "week", "month", "year", "weekday")

    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data + "time")

    # read in song data to use for songplays table
    songs_df = spark.read.parquet(os.path.join(output_data, "songs/*/*/*"))
    songs_logs = log_df.join(songs_df, (log_df.song == songs_df.title))

    # extract columns from joined song and log datasets to create songplays table
    artists_df = spark.read.parquet(os.path.join(output_data, "artists"))
    artists_songs_logs = songs_logs.join(artists_df, (songs_logs.artist == artists_df.name))
    songplays = artists_songs_logs.join(
        time_table,
        artists_songs_logs.ts == time_table.ts, 'left'
    ).drop(artists_songs_logs.year)

    # cherry pick columns for songplays
    songplays_table = songplays.select(
        col('start_time'),
        col('userId').alias('user_id'),
        col('level'),
        col('song_id'),
        col('artist_id'),
        col('sessionId').alias('session_id'),
        col('location'),
        col('userAgent').alias('user_agent'),
        col('year'),
        col('month'),
    ).repartition("year", "month")
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data, 'songplays')



def main():
    
    # creates spark session
    spark = create_spark_session()
    
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-lake/"
    
    # calls method for processing song_data
    process_song_data(spark, input_data, output_data)    
    
    # calls method for processing log_data
    process_log_data(spark, input_data, output_data)
    spark.stop()


if __name__ == "__main__":
    
    # kickstarts execution
    main()
