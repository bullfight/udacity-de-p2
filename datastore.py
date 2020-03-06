import cassandra
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

class DataStore:
    def __init__(self):
        self.session = self.setup_session()
        self.setup_keyspace()

    def setup_session(self):
        auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
        cluster = Cluster(auth_provider=auth_provider)
        session = cluster.connect()
        return session

    def setup_keyspace(self):
        self.session.execute(
            """
                CREATE KEYSPACE IF NOT EXISTS udacity
                WITH REPLICATION =
                { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }
            """
        )

        self.session.set_keyspace('udacity')

    def execute(self, sql, data=None):
        result = self.session.execute(sql, data)
        return result

create_table_query = """
    CREATE TABLE IF NOT EXISTS music_library
    (
        year        int,
        artist_name text,
        album_name  text,
        city text,  PRIMARY KEY (year, artist_name, album_name)
    )
"""

drop_table_query = """
    DROP TABLE IF EXISTS music_library
"""

insert_query = """
    INSERT INTO music_library (year, artist_name, album_name, city) VALUES (%s, %s, %s, %s)
"""

query = "SELECT * FROM music_library WHERE year = 1970"

store = DataStore()
store.execute(create_table_query)
store.execute(insert_query, (1970, "The Beatles", "Let it Be", "Liverpool"))

results = store.execute(query)
print(results.one())

store.execute(drop_table_query)
