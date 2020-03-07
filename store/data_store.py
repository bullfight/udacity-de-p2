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
