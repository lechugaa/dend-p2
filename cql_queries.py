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
    CREATE TABLE IF NOT EXISTS song_library_table (
        artist text,
        title text,
        length float,
        session_id int,
        item_in_session int,
        PRIMARY KEY (session_id, item_in_session)
    );
"""

query_2_table_create = """
    CREATE TABLE IF NOT EXISTS user_reproductions_table (
        artist text,
        title text,
        first_name text,
        last_name text,
        user_id int,
        session_id int,
        item_in_session int,
        PRIMARY KEY (user_id, session_id, item_in_session)
    );
"""

query_3_table_create = """
    CREATE TABLE IF NOT EXISTS song_users_table (
        first_name text,
        last_name text,
        title text,
        PRIMARY KEY (title, first_name, last_name)
    );
"""

# INSERT RECORDS
query_1_table_insert = """
    INSERT INTO song_library_table (artist, title, length, session_id, item_in_session)
    VALUES (%s, %s, %s, %s, %s);
"""

query_2_table_insert = """
    INSERT INTO user_reproductions_table (artist, title, first_name, last_name, user_id, session_id, item_in_session)
    VALUES (%s, %s, %s, %s, %s, %s, %s);
"""

query_3_table_insert = """
    INSERT INTO song_users_table (first_name, last_name, title)
    VALUES (%s, %s, %s);
"""

# SELECT QUERIES
select_query_1 = """
    SELECT artist, title, length FROM song_library_table 
    WHERE session_id = 338 AND item_in_session = 4;
"""

select_query_2 = """
    SELECT artist, title, first_name, last_name FROM user_reproductions_table 
    WHERE user_id = 10 AND session_id = 182;
"""

select_query_3 = """
    SELECT first_name, last_name FROM song_users_table 
    WHERE title = 'All Hands Against His Own';
"""

# DROP TABLES
query_1_table_drop = "DROP TABLE IF EXISTS song_library_table;"
query_2_table_drop = "DROP TABLE IF EXISTS user_reproductions_table;"
query_3_table_drop = "DROP TABLE IF EXISTS song_users_table;"

# QUERIES LIST
create_table_queries = [query_1_table_create, query_2_table_create, query_3_table_create]
drop_table_queries = [query_1_table_drop, query_2_table_drop, query_3_table_drop]
