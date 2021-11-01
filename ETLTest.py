import unittest
import os
from dotenv import load_dotenv
from create_tables import get_cassandra_connection
from cql_queries import select_query_1, select_query_2, select_query_3


class ETLTest(unittest.TestCase):
    def setUp(self):
        """
        Sets up Cassandra cluster, session and keyspace
        """
        load_dotenv()
        self.cluster, self.session = get_cassandra_connection()
        self.session.set_keyspace(os.getenv('KEYSPACE'))

    def tearDown(self):
        """
        Closes Cassandra's connections
        """
        self.session.shutdown()
        self.cluster.shutdown()

    def test_table_1_query(self):
        """
        Tests desired query
        Give me the artist, song title and song's length in the music app history that was heard during sessionId = 338,
        and itemInSession = 4
        """
        row = self.session.execute(select_query_1).one()
        self.assertEqual(row.artist, 'Faithless', 'Obtained incorrect artist from query!')
        self.assertEqual(row.title, 'Music Matters (Mark Knight Dub)', 'Obtained incorrect title from query!')
        self.assertAlmostEqual(row.length, 495.30731201171875, 3, 'Obtained incorrect length from query!')

    def test_table_2_query(self):
        """
        Tests desired query
        Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for
        userid = 10, sessionid = 182
        """
        rows = self.session.execute(select_query_2)
        expected_rows = [
            {'artist': 'Down To The Bone', 'title': "Keep On Keepin' On", 'first_name': 'Sylvie', 'last_name': 'Cruz'},
            {'artist': 'Three Drives', 'title': 'Greece 2000', 'first_name': 'Sylvie', 'last_name': 'Cruz'},
            {'artist': 'Sebastien Tellier', 'title': 'Kilometer', 'first_name': 'Sylvie', 'last_name': 'Cruz'},
            {'artist': 'Lonnie Gordon', 'title': 'Catch You Baby (Steve Pitron & Max Sanna Radio Edit)', 'first_name': 'Sylvie', 'last_name': 'Cruz'}
        ]

        for row, expected_row in zip(rows, expected_rows):
            self.assertEqual(row.artist, expected_row['artist'], 'Obtained incorrect artist from query!')
            self.assertEqual(row.title, expected_row['title'], 'Obtained incorrect title from query!')
            self.assertEqual(row.first_name, expected_row['first_name'], 'Obtained incorrect first name from query!')
            self.assertEqual(row.last_name, expected_row['last_name'], 'Obtained incorrect last name from query!')

    def test_table_3_query(self):
        """
        Tests desired query
        Give me every user name (first and last) in my music app history who listened to the song
        'All Hands Against His Own'
        """
        rows = self.session.execute(select_query_3)
        expected_rows = [
            {'first_name': 'Jacqueline', 'last_name': 'Lynch'},
            {'first_name': 'Sara', 'last_name': 'Johnson'},
            {'first_name': 'Tegan', 'last_name': 'Levine'}
        ]
        for row, expected_row in zip(rows, expected_rows):
            self.assertEqual(row.first_name, expected_row['first_name'], 'Obtained incorrect first name from query!')
            self.assertEqual(row.last_name, expected_row['last_name'], 'Obtained incorrect last name from query!')


if __name__ == '__main__':
    unittest.main()
