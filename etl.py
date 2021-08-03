#%%
import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DoubleType, LongType
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

#%%
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config["AWS"]['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config["AWS"]['AWS_SECRET_ACCESS_KEY']
os.environ['jdk.xml.entityExpansionLimit']= '0' 

#%%
song_schema = StructType([
    StructField('artist_id', StringType()),
    StructField('artist_latitude', DoubleType()),
    StructField('artist_location', StringType()),
    StructField('artist_longitude', DoubleType()),
    StructField('artist_name', StringType()),
    StructField('duration', DoubleType()),
    StructField('num_songs', IntegerType()),
    StructField('song_id', StringType()),
    StructField('title', StringType()),
    StructField('year', IntegerType())
])

#%%
def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0") \
        .config("spark.hadoop.fs.s3a.multipart.size", "104857600") \
        .getOrCreate()
    return spark

#%%
def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = f"{input_data}song-data/*/*/*/*.json"

    # read song data file
    df = spark.read.json(song_data, schema=song_schema)

    # extract columns to create songs table
    songs_table = df.select('artist_id', 'duration', 'song_id', 'title', 'year')\
        .withColumnRenamed("song_id", "id")\
        .dropDuplicates(["id", 'title'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id")\
        .mode("overwrite")\
        .parquet(f'{output_data}songs.parquet')

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_latitude', 'artist_location', 'artist_longitude', 'artist_name')\
        .toDF('id', 'latitude', 'location', 'longitude', 'name')\
        .dropDuplicates(['id', 'name'])
    
    # write artists table to parquet files
    artists_table.write.parquet(f"{output_data}artists.parquet")

#%%
def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = f"{input_data}log-data/*.json"

    log_schema = StructType([
        StructField('artist', StringType()),
        StructField('auth', StringType()),
        StructField('firstName', StringType()),
        StructField('gender', StringType()),
        StructField('itemInSession', IntegerType()),
        StructField('lastName', StringType()),
        StructField('length', DoubleType()),
        StructField('level', StringType()),
        StructField('location', StringType()),
        StructField('method', StringType()),
        StructField('page', StringType()),
        StructField('registration', DoubleType()),
        StructField('sessionId', LongType()),
        StructField('song', StringType()),
        StructField('status', IntegerType()),
        StructField('ts', LongType()),
        StructField('userAgent', StringType()),
        StructField('UserId', StringType())
    ])

    # read log data file
    df = spark.read.json(log_data, schema=log_schema)
    
    # filter by actions for song plays
    df = df.filter("page == 'NextSong'")

    # extract columns for users table    
    users_table = df.select('firstName', 'gender', 'lastName', 'level', 'location', 'UserId')\
        .toDF('first_name', 'gender', 'last_name', 'level', 'location', 'id')\
        .dropDuplicates()
    
    # write users table to parquet files
    users_table.write.partitionBy('gender').parquet(f"{output_data}users.parquet")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp( int(x) )))
    df = df.withColumn('datetime', get_datetime(df.timestamp))
    
    # extract columns to create time table
    #year, month, dayofmonth, hour, weekofyear, date_format
    time_table = df.select('datetime') \
        .drop_duplicates() 

    time_table = time_table.withColumn('year', year(time_table.datetime))\
        .withColumn('month', month(time_table.datetime))\
        .withColumn('dayofmonth', dayofmonth(time_table.datetime))\
        .withColumn('hour', hour(time_table.datetime))\
        .withColumn('weekofyear', weekofyear(time_table.datetime))\
        .withColumn('weekofyear', weekofyear(time_table.datetime))
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", 'month').parquet(f"{output_data}time_table.parquet")

    # read in song data to use for songplays table
    song_df = spark.read.json(f"{input_data}song-data/*/*/*/*.json", schema=song_schema)

    # extract columns from joined song and log datasets to create songplays table 
    #https://sparkbyexamples.com/pyspark/pyspark-join-explained-with-examples/
    df = df.alias('df')
    song = song_df.alias('song')

    songplays_table = df.join(song, 
        on=[df.artist == song.artist_name,
            df.song == song.title],
        how='inner'
    )

    songplays_table = songplays_table.select(
            col('df.sessionId').alias('session_id'),
            col('df.location').alias('location'), 
            col('df.userAgent').alias('user_agent'),
            col('df.datetime').alias('start_time'),
            col('df.userId').alias('user_id'),
            col('df.level').alias('level'),
            col('song.song_id').alias('song_id'),
            col('song.artist_id').alias('artist_id'),
            year('df.datetime').alias('year'),
            month('df.datetime').alias('month')) \
            .dropDuplicates() \
            .withColumn('songplay_id', monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", 'month').parquet(f"{output_data}songplays.parquet")

#%%
def main():
    spark = create_spark_session()

    env = os.environ.get('LOCAL', 'PRD').upper()
    data_config = f'{env}_DATA'
    input_data = config[data_config]['input_data']
    output_data = config[data_config]['output_data']
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

#%%
if __name__ == "__main__":
    main()
