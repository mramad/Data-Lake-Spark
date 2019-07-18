import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

# Help from Slack was obtained whereever required

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
     """Creates a Spark session"""
     spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
     return spark


def process_song_data(spark, input_data, output_data):
     """loads JSON files from song directory at S3, Process the data and writes it back to parquet files at S3"""
     # get filepath to song data file
     song_data = input_data
    
     # read song data files
     df = spark.read.json(song_data)
    
     # create SQL Spark table to process song data
    
     df.createOrReplaceTempView("song_met_table")

     # extract columns to create songs table
     song_table=spark.sql('''SELECT distinct song_id,title,artist_id,year,duration
                          FROM song_met_table
                          ''')
    
    
     # write songs table to parquet files partitioned by year and artist
     song_table.write.partitionBy('year','artist_id').parquet(os.path.join( output_data,'song'))

     # extract columns to create artists table
     artist_table=spark.sql('''SELECT distinct artist_id,artist_name,artist_location,artist_latitude,artist_longitude
                              FROM song_met_table
                              ''')
    
     # write artists table to parquet files
     artist_table.write.parquet(os.path.join( output_data,'artist'))

def process_log_data(spark, input_data, output_data):
    """loads JSON files from log directory at S3, Process the data and writes it back to parquet files at S3"""
    # get filepath to log data files
    log_data = input_data

    # read log data file
    df = spark.read.json(log_data)

    # create SQL Spark table to process log data
    
    df.createOrReplaceTempView("log_met_table")
    
    # filter by actions for song plays
    df =  spark.sql('''SELECT *
                    FROM log_met_table
                    where page = 'NextSong'
                    ''')

    # extract columns for users table    
    users_table = spark.sql('''SELECT distinct userId,firstName,lastName,gender,level
                               FROM log_met_table
                               where page = 'NextSong'
                            ''')
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join( output_data,'users'))

    # create timestamp column from original timestamp column
    spark.udf.register("get_timestamp", lambda x: datetime.fromtimestamp(x/1000.0).strftime('%Y-%m-%d %H:%M:%S'))
    
    
    # extract columns to create time table
    time_table = spark.sql('''
                           SELECT distinct get_timestamp(ts) AS starttime,hour(get_timestamp(ts)) as hour,day(get_timestamp(ts)) as day,
                           weekofyear(get_timestamp(ts)) as week, month(get_timestamp(ts)) as month,year(get_timestamp(ts)) as year,
                          dayofweek(get_timestamp(ts)) as weekday
                          FROM log_met_table 
                           ''' )
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year','month').parquet(os.path.join( output_data,'time'))



    # extract columns from joined song and log datasets to create songplays table 
    songplay_table=spark.sql('''SELECT get_timestamp(ts) AS starttime,year(get_timestamp(ts)) AS year,month(get_timestamp(ts)) AS                                         month,userId,level,song_id,artist_id,sessionid,location,userAgent
                                FROM log_met_table lg
                                JOIN song_met_table  st on (lg.song=st.title and lg.artist=st.artist_name)
                                WHERE page = 'NextSong'
                             ''')

    # write songplays table to parquet files partitioned by year and month
    songplay_table.write.partitionBy('year','month').parquet(os.path.join( output_data,'songplay'))




def main():
    spark = create_spark_session()
    input_data1 = "s3a://udacity-dend/song_data/*/*/*/*.json"
    input_data2 = "s3a://udacity-dend/log_data/*/*/*.json"
    output_data = "s3a://dlproject-mar/output/"
    
    process_song_data(spark, input_data1, output_data)    
    process_log_data(spark, input_data2, output_data)


if __name__ == "__main__":
    main()
