# Sparkify Song play database

Sparkify presently has no way to review or analyse their user and song data.  At the moment all of the user and song information is stored in JSON files in an AWS S3 bucket.


## Motivation

The Sparkify streaming company want to start analysing thier user behaviours. The behaviours can theb grouped by subscription level, gender, location or any other grouping that data anlaysis team can use.

## Technical Specs
**Built with:**
     *Python
     *SQL
     *Redshift
     *S3

The database will use a star schema that will be optimized for song play analysis. 

The database will be centered on a fact table called song plays.  This will then be conenct to a number of dimension tables. The dimension tables will hodl the data for the songs, users , artists and time.

The JSON fiels are stored in an AWS S3 bucket.  The database will be hosted on a multi-node(4) Redshift server

The python files are run through the terminal.  the create_tables.py must be run first to create the tables and to clear out any existing bad data in the database.
Then the etl.py is run to process the JSON files
example: python create_tables.py
         python etl.py
         python sql_queries.py

### Description of files
sql_queries.py - Python file has all of the nessary sql scripts to run create, drop and insert into the tables for the sparkify database 
create_tables.py - Python file that creates the conection to the database and creates the tables for the sparkify database 
etl.py - Python file that creates the conection to the database and runs sql queries that will populate the staging tables from the JSON files and then move the data into the star schema tables.
README.md - descrition of project, the files in the project and how to run the files.  


## Sample Queries
    This query looks at the number of paid and free plans by gender 
    1. select count(users.user_id),users.level,users.gender FROM users GROUP BY users.level,users.gender
    ![Results from Query 1](result1.png)
    
    This query looks at the number of songs played by user and if that user is on the free plan or the paid plan
    2. SELECT count(songplays.user_id), users.user_id, users.first_name,users.last_name,users.level  FROM songplays join users on songplays.user_id = users.user_id  group by users.user_id, users.first_name,users.last_name,users.level order by users.level desc
    ![Results from Query 2](result2.png)
    

    This query looks at the data quality of the song plays table.This verify if ther is any missing sng or artist ids.
    3.select songplays.song_id ,songplays.artist_id from songplays where  songplays.song_id is nullor songplays.artist_id is null;
    ![Results from Query 2](result3.png)
    
    