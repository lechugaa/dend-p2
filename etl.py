import os
import csv
from cassandra.cluster import Session
from dotenv import load_dotenv
from create_tables import get_cassandra_connection
from cql_queries import query_1_table_insert, query_2_table_insert, query_3_table_insert


def process_full_csv(session: Session):
    """
    Processes the csv compilation and adds the necessary records to the
    databases tables per each csv row.
    :param session: Apache Cassandra Session
    """
    file = os.getenv('FULL_CSV_NAME')
    with open(file, encoding='utf8') as f:
        csv_reader = csv.reader(f)
        next(csv_reader)  # skip header
        for line in csv_reader:
            # table 1 insertion
            session.execute(query_1_table_insert, (line[0], line[9], float(line[5]), int(line[8]), int(line[3])))
            # table 2 insertion
            session.execute(query_2_table_insert, (line[0], line[9], line[1], line[4], int(line[10]), int(line[8]), int(line[3])))
            # table 3 insertion
            session.execute(query_3_table_insert, (line[1], line[4], int(line[10]), line[9]))


def main():
    """
    Performs the following tasks:
        - Gets an Apache Cassandra Connection and sets the project's keyspace
        - Inserts records into tables
        - Closes connection and stops cluster
    """
    # loading .env variables
    load_dotenv()

    # starting cluster and session
    cluster, session = get_cassandra_connection()

    # setting keyspace
    session.set_keyspace(os.getenv('KEYSPACE'))

    # inserting rows into tables
    process_full_csv(session)

    # closing connections
    session.shutdown()
    cluster.shutdown()


if __name__ == '__main__':
    main()
