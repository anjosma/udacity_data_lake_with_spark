# Data Lake
This is the fourth project from Nanodegree Data Engineering from Udacity. The project aims to create and modeling an Data Lake in Amazon EMR environment in a startup called Sparkfy using `PySpark`. Also, it uses S3 to storage `json` files as input and `parquet` as output.

## Data
The data we use is available in two differents folders in S3 bucket : `s3://udacity-dend/log-data` and `s3://udacity-dend/song-data` whether running in AWS or `./data/input/log-data` and `./data/input/song-data` wheter running locally.

## About the tables
### Fact Table
**songplays** - records in event data associated with song plays i.e. records with page NextSong:
- songplay_id, start_time, year, month, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension Tables
**users** - users in the app:
- user_id, first_name, last_name, gender, level  

**songs** - songs in music database:
- song_id, title, artist_id, year, duration  

**artists** - artists in music database:
- artist_id, name, location, latitude, longitude  

**time** - timestamps of records in songplays broken down into specific units:
- start_time, hour, day, week, month, year, weekday  

## How run the project?
### Steps
- `etl.py`

First, clone this GIT repository into your local machine and install the requirements libraries using:
```console
$ pip install -r requirements.txt
```
Now, the environment is ready! 

Before running the scripts, we need to fill the configuration file `dwh.cfg` with database information and `ARN` string from Amazon Web Services.
```ini
[AWS]
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=

[PRD_DATA]
input_data=s3a://udacity-dend/
output_data=s3a://udacity-datalake-output/

[LOCAL_DATA]
input_data=./data/input/
output_data=./data/output/
```
If you are running the code locally, set a environment variable `ENV` with value `local`, otherwise the script will try to get data in S3:
```console
$ export ENV=local
```

Execute the following script:
```console
$ python3 etl.py
```
The command above will create all the tables from S3 structured folders into tables in Spark and save again in S3 or locally. 
