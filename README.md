## Project: AWS Redshift Programmatic Data Engineering and Modeling

In this project, we develop a relational database and an ETL pipeline to store transactional song information. The expected business use is for the Sparkify analytics teams to understand the songs users are listening to, and develop better analytics around these customers. This RDBMS will help give visibility to their business (favorite songs, how many users, ...) and the ability to drive new product offerings, investigate problems, and better reporting. This database will allow flexibility in querying information depending on the analysts needs. The ETL process does all the "dirty work" and allows the analysts to focus on data insights rather than data wrangling.

As a data engineer, we use a semi-automated ETL process for organizing the log/song JSON data. The ETL pipeline is developed to interact to S3 and be hosted on an AWS Redshift relation database. 

We programmatically connect to the existing AWS Redshift Cluster, creating our tables. The ETL process takes files from a S3 ( cloud storage) and loads them into the staging tables, and finally our production tables in our database. We define a fact and dimension tables in a star schema, for an analytical focus. The star schema is a simple strategy, that allows for data integrity, intuitive organization, and SQL queries by multiple analysts. The database structure is as follows (star schema):


**Fact Table**
1. **songplays** - records in log data associated with song plays i.e. records with page NextSong
	- songplay_id, timestamp, user_id, level, song_id, artist_id, session_id, location, user_agent

**Dimension Tables**
1. **users** - users in the app
	- user_id, first_name, last_name, gender, level
2. **songs** - songs in music database
	- song_id, title, artist_id, year, duration
3. **artists** - artists in music database
	- artist_id, name, location, latitude, longitude
4. **time** - timestamps of records in songplays broken down into specific units
	- start_time, hour, day, week, month, year, weekday

**Staging Tables**
1. staging_events
2. staging_songs

**Temporary Tables**
1. staging_time (this is a temporary table for time calculations)

## Instructions
1. Create AWS Redshift cluster, store ARN in the dwh.cfg (and needed access information).
2. Run **sql_queries.py**
3. Run **create_tables.py**
4. Run **etl.py**
5. Can use **Querying_The_AWS_Redshift_Cluster.ipynb** to connect and execute sample queries

## Files
- **create_tables.py** - connects to the Sparkify database, drops any tables if they exist, and creates the tables.
- **sql_queries.py** -  specify all columns for each of the staging and five tables with the right data types and conditions. Contains SQL for drop, create, insert SQL statements.
- **etl.py** - this script connects to the Sparkify Redshift database, loads log_data and song_data into staging tables, and transforms them into the five tables.
- **dwh.cfg** - Configuration file; needs access information and file paths to S3 if changes.
- **Querying_The_AWS_Redshift_Cluster.ipynb** - sample queries accessing cluster. 
- **README.md** - discusses the project 
- **Development.ipynb** - this is a developmental notebook for the project (works completely, currently ignored). 

