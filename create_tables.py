import os
from cassandra.cluster import Cluster, Session
from cassandra.auth import PlainTextAuthProvider
from cql_queries import keyspace_drop, keyspace_create, create_table_queries, drop_table_queries
from dotenv import load_dotenv


def get_cassandra_connection():
    """
    Returns an Apache Cassandra Cluster and Session
    :return: Apache Cassandra Session
    """
    auth_provider = PlainTextAuthProvider(
        username=os.getenv('CASSANDRA_PASSWORD'),
        password=os.getenv('CASSANDRA_USER')
    )
    cluster = Cluster(auth_provider=auth_provider)
    session = cluster.connect()

    return cluster, session


def create_keyspace(session: Session):
    """
    Creates the etl keyspace and sets it as the session's keyspace.
    :param session: Apache Cassandra Session
    """
    keyspace_name = os.getenv('KEYSPACE')
    session.execute(keyspace_drop.format(keyspace_name))
    session.execute(keyspace_create.format(
        keyspace_name,
        os.getenv('REPLICATION_STRATEGY'),
        os.getenv('REPLICATION_FACTOR')
    ))
    session.set_keyspace(keyspace_name)


def drop_tables(session: Session):
    """
    Drops the tables that will be used in the etl process if they exist.
    :param session: Cassandra Apache Session
    """
    for query in drop_table_queries:
        session.execute(query)


def create_tables(session: Session):
    """
    Creates the tables that will be used in the etl process if they do not exist.
    :param session: Apache Cassandra Session
    """
    for query in create_table_queries:
        session.execute(query)


def main():
    """
    Performs the following tasks:
        - Obtains and authenticated Apache Cassandra Session
        - Creates the ETL's keyspace
        - Deletes any table using the same names that will be used in the ETL
        - Generates the required tables
    """
    cluster, session = get_cassandra_connection()
    create_keyspace(session)
    drop_tables(session)
    create_tables(session)
    session.shutdown()
    cluster.shutdown()


if __name__ == '__main__':
    load_dotenv()
    main()
