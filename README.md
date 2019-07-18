# Data-Lake-Spark
DataLake | Python | Spark

Project Description:

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

ETL Pipeline:

This Project builds an ETL pipeline that extracts Sparkify song and log data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables represented as parquet files. This will allow their analytics team to continue finding insights in what songs their users are listening to.

Spark SQL API was used for data wrangling and transformation.

S3:
A datalake S3 Bucket "dlproject-mar" has been created to host the output parquet files.
Each output table will be stored in its own directory under the S3 bucket, the directory will be named after the table name.

Database Design:

Fact Table:
Single Fact table "songplays" that hosts user activity data is created, data is extracted from the log data, filtered by "NextSong" action and then merged with data from dimension tables in order to retreive data required for the fact table but does not exist in log data.
        Table Columns: start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

Dimension tables:
    - Users: stores unique records for  users of the app
        Table Columns:user_id, first_name, last_name, gender, level
    - Songs: stores unique records for the songs registered in the App.
        Table Columns:
    - Artists: stores unique records for artists 
    - Time : Stores timestamps of records in songplays table and time unit extracted from timestamp data.
        Table Columns: start_time, hour, day, week, month, year, weekday

The Project has 2 files detailed as the following:

dwh.cfg: This file has the AWS cloud configuration data as the following:
    -config data for AWS access (This has been removed for secuitry purposes)
    
etl_AWS.py: This file has the code to perform the following:
    - connect to Amazon S3 to retreive the App log and song data
    - Process data using Spark SQL APIs to create required tables and perform necessary transformations
    - Writes the tables back to parquet files into a special created Amazon S3 bucket "dlproject-mar"


Project Run:

- Run etl_AWS.py, this should connect to S3,load and transform data and write it back to S3 bucket.
 
