# DataWareHouse-Using-Redshift
Design and Implementation of Data Warehouse using AWS Redshift Cluster

Project Overview :-
  This Project handles data of a music streaming startup, Sparkify. Data set is a set of files in JSON format stored in AWS S3 buckets and contains two parts:

-- > s3://udacity-dend/song_data: static data about artists and songs Song-data example: {"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}

-- > s3://udacity-dend/log_data: event data of service usage e.g. who listened what song, when, where, and with which client

Project builds an ETL pipeline (Extract, Transform, Load) to create the DB and tables in AWS Redshift cluster, fetch data from JSON files stored in AWS S3, process the data, and insert the data to AWS Redshift DB. As technologies, Project-3 uses python, SQL, AWS S3 and AWS Redshift DB.

AWS Redshift set-up
AWS Redshift is used in ETL pipeline as the DB solution. Used set-up in the Project-3 is as follows:

Cluster: 4x dc2.large nodes
Location: US-West-2 (as Project-3's AWS S3 bucket)
Staging tables

Fact Table
songplays: song play data together with user, artist, and song info (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
Dimension Tables
users: user info (columns: user_id, first_name, last_name, gender, level)
songs: song info (columns: song_id, title, artist_id, year, duration)
artists: artist info (columns: artist_id, name, location, latitude, longitude)
time: detailed time info about song plays (columns: start_time, hour, day, week, month, year, weekday)


Structure
The project contains the following components:

create_tables.py
  creates the Sparkify star schema in Redshift
etl.py 
  defines the ETL pipeline, extracting data from S3, loading into staging tables on Redshift, and then processing into analytics tables on Redshift
sql_queries.py 
  defines the SQL queries that underpin the creation of the star schema and ETL pipeline
dws_redshift.ipynb 
  allows you to more interactively execute the ETL and run queries
  
Run create_tables.py
Type to command line:

python3 create_tables.py

All tables are dropped.
New tables are created: 2x staging tables + 4x dimensional tables + 1x fact table.
Output: Script writes "Tables dropped successfully" and "Tables created successfully" if all tables were dropped and created without errors.
Run etl.py
Type to command line:

python3 etl.py

Script executes AWS Redshift COPY commands to insert source data (JSON files) to DB staging tables.
From staging tables, data is further inserted to analytics tables.
Script writes to console the query it's executing at any given time and if the query was successfully executed.
In the end, script tells if whole ETL-pipeline was successfully executed.  
  
  
  
