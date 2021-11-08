"""
cql_queries.py

This python script isolates all the queries that are needed so they can be imported
by other scripts.
"""

# CREATE KEYSPACE
keyspace_create = """
    CREATE KEYSPACE IF NOT EXISTS {} 
    WITH REPLICATION = {{'class': '{}', 'replication_factor': '{}'}};
"""

# DROP KEYSPACE
keyspace_drop = "DROP KEYSPACE IF EXISTS {};"

# CREATE TABLES
query_1_table_create = """
    CREATE TABLE IF NOT EXISTS songs_per_session_and_item (
        artist text,
        title text,
        length float,
        session_id int,
        item_in_session int,
        PRIMARY KEY (session_id, item_in_session)
    );
"""

query_2_table_create = """
    CREATE TABLE IF NOT EXISTS songs_per_user_and_session (
        artist text,
        title text,
        first_name text,
        last_name text,
        user_id int,
        session_id int,
        item_in_session int,
        PRIMARY KEY ((user_id, session_id), item_in_session)
    );
"""

query_3_table_create = """
    CREATE TABLE IF NOT EXISTS users_per_song (
        first_name text,
        last_name text,
        user_id int,
        title text,
        PRIMARY KEY (title, user_id)
    );
"""

# INSERT RECORDS
query_1_table_insert = """
    INSERT INTO songs_per_session_and_item (artist, title, length, session_id, item_in_session)
    VALUES (%s, %s, %s, %s, %s);
"""

query_2_table_insert = """
    INSERT INTO songs_per_user_and_session (artist, title, first_name, last_name, user_id, session_id, item_in_session)
    VALUES (%s, %s, %s, %s, %s, %s, %s);
"""

query_3_table_insert = """
    INSERT INTO users_per_song (first_name, last_name, user_id, title)
    VALUES (%s, %s, %s, %s);
"""

# SELECT QUERIES
select_query_1 = """
    SELECT artist, title, length FROM songs_per_session_and_item 
    WHERE session_id = 338 AND item_in_session = 4;
"""

select_query_2 = """
    SELECT artist, title, first_name, last_name FROM songs_per_user_and_session 
    WHERE user_id = 10 AND session_id = 182;
"""

select_query_3 = """
    SELECT first_name, last_name FROM users_per_song 
    WHERE title = 'All Hands Against His Own';
"""

# DROP TABLES
query_1_table_drop = "DROP TABLE IF EXISTS songs_per_session_and_item;"
query_2_table_drop = "DROP TABLE IF EXISTS songs_per_user_and_session;"
query_3_table_drop = "DROP TABLE IF EXISTS users_per_song;"

# QUERIES LIST
create_table_queries = [query_1_table_create, query_2_table_create, query_3_table_create]
drop_table_queries = [query_1_table_drop, query_2_table_drop, query_3_table_drop]
