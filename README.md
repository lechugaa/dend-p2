# Sparkify: Data Modeling with Apache Cassandra

This project is part of Udacity's **Data Engineering Nanodegree**. 

## Description

This project is an ETL pipeline using Python that helps a fake startup called Sparkify generate the databases required 
by its analytics team. As this team is particularly interested in three specific queries prioritizing availability in
face of a network partition, Apache Cassandra was used and a table was designed for each query. The desired queries
are:

1. Obtain the artist, song title and song's length in the music app history that was heard during and specific session and item in session.
2. Obtain the name of artist, song (sorted by itemInSession) and user (first and last name) for a specific user and session.
3. Obtain every user name (first and last names) in the music app history who listened to a specific song.

## Project Structure

```
├── README.md                          <- The top-level README for developers using this project.
├── even_data                          <- Directory for csv files to process
├── images                             <- Directory for required images
├── .gitignore                         <- Git configuration file to ignore specific directories in VC
├── etl.ipynb                          <- Jupyter notebook used as draft to build the ETL process
├── test.ipynb                         <- Jupyter notebook used to test schema design queries
├── cql_queries.py                     <- Contains all the Cassandra queries using by the project's scripts
├── create_tables.py                   <- Script in charge of creting the keyspace and the required tables
├── etl.py                             <- Python script responsible of populating the db
├── ETLTest.py                         <- UnitTests for the ETL process
├── preprocessing.py                   <- Python script responsible of consolidating the multiple csv files into one
├── .env.example                       <- Template used to generate the required .env file
└── requirements.txt                   <- Library dependencies needed for the pipeline
```

## Keyspace Design

Due to the analytical purposes of this database, three different tables were designed to address each of the desired 
queries.

### Song Library Table

This tables helps the analytics team find a song when they have knowledge of the session and the item in session. It has
the following structure:

|       Column      |   Type  |
|:-----------------:|:-------:|
|      `artist`     |  `text` |
|      `title`      |  `text` |
|      `length`     | `float` |
|    `session_id`   |  `int`  |
| `item_in_session` |  `int`  |

`PRIMARY KEY` is `(session_id, item_in_session)`.

### User Reproductions Table

This tables helps the analytics team find the name of the artist, song and user when they have knowledge of the 
user id and the session id. The results are returned in ascending order of the item in session column. It has the 
following structure:

| Column            | Type   |
|:-----------------:|:------:|
| `artist`          | `text` |
| `title`           | `text` |
| `first_name`      | `text` |
| `last_name`       | `text` |
| `user_id`         | `int`  |
| `session_id`      | `int`  |
| `item_in_session` | `int`  |

`PRIMARY KEY` is `(user_id, session_id, item_in_session)`.

### Song Users Table

This tables helps the analytics team find the first and last name of the users that have listened to a specific song. 
It has the following structure:

| Column            | Type   |
|:-----------------:|:------:|
| `first_name`      | `text` |
| `last_name`       | `text` |
| `title`           | `text` |

`PRIMARY KEY` is `(title, first_name, last_name)`. As we do not wish to record the number of times a user has listened
to a song, but rather if they have or not, these fields are enough for the primary key and it helps us reduce the 
occupied space by this table.


## ETL Pipeline

The developed pipeline reads the `event_data_new.csv` file that is generated by the `preprocessig.py` script by 
joining all the files contained in the `event_data` directory.

To process the `event_data_new.csv` file, the `etl.py` script inserts the correct fields as records for every of
the tables discussed above.

## Requirements

### Environment Variables

This project uses a `.env` file with the following structure:

```
CASSANDRA_USER=
CASSANDRA_PASSWORD=
KEYSPACE=
REPLICATION_STRATEGY=
REPLICATION_FACTOR=
DATA_DIRECTORY=
FULL_CSV_NAME=
```

To help the user create its own `.env` file, a `.env.example` file is provided with the default values. To rapidly
generate the environment file run the following command:

```shell
cp .env.example .env
```

If the developer wishes to change any of the default configurations (such as the authentication credentials for the 
Cassandra backend) the `.env` file is the single source of truth for the whole project.

### Apache Cassandra

This project uses [Apache Cassandra](https://cassandra.apache.org/_/index.html) as its database backend. This project
assumes you already have the backend installed on the running computer.

### Python

Python 3.9.4 is required to execute the scripts of this project. The library dependencies have
been extracted to the file called `requirements.txt` and can be installed used the following command:

```
pip install -r requirements.txt
```

## Generating the database

In order to create the databases clone this repo and once you have fulfilled the requirements detailed above run the 
following commands:

```shell
python preprocessing.py
```

```shell
python create_tables.py
```

```shell
python etl.py
```

## Testing Tables

To verify that the tables have been created and populated successfully, run the UnitTests included with the repo. All
tests should pass. To run them use the following command:

```shell
python -m unittest ETLTest.py   
```

## Sample Queries

To review the design queries, you can consult the `test.ipynb`. There the following sample queries are executed:

**What user level is having the most repoductions?**

```sql
SELECT artist, title, length FROM song_library_table 
WHERE session_id = 338 AND item_in_session = 4;
```

The results are:

|   artist  |               song              |   length   |
|:---------:|:-------------------------------:|:----------:|
| Faithless | Music Matters (Mark Knight Dub) | 495.307312 |

**Which are the top ten locations with the most reproductions?**

```sql
SELECT artist, title, first_name, last_name FROM user_reproductions_table 
WHERE user_id = 10 AND session_id = 182;
```
The results are:

|       artist      |                       title                       | first_name | last_name |
|:-----------------:|:-------------------------------------------------:|:----------:|:---------:|
| Down To The Bone  | Keep On Keepin' On                                | Sylvie     | Cruz      |
| Three Drives      | Greece 2000                                       | Sylvie     | Cruz      |
| Sebastien Tellier | Kilometer                                         | Sylvie     | Cruz      |
| Lonnie Gordon     | Catch You Baby (Steve Pitron & Max Sanna Radio... | Sylvie     | Cruz      |

**What day of the week are we most listened to?**

```sql
SELECT first_name, last_name FROM song_users_table 
WHERE title = 'All Hands Against His Own';
```

The results are:

| first_name | last_name |
|:----------:|:---------:|
| Jacqueline |     Lynch |
|       Sara |   Johnson |
|      Tegan |    Levine |