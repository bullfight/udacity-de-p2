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

    def create_table(self):
        self.execute(self.create_table_query)

    def drop_table(self):
        query = "DROP TABLE IF EXISTS {}".format(self.table_name)
        self.execute(query)

    def create(self, data):
        keys = list(data.keys())
        names = ', '.join(keys)
        insert_string = ['%s'] * len(keys)
        insert_string = ', '.join(insert_string)
        query = "INSERT INTO {} ({}) VALUES ({})".format(self.table_name, names, insert_string)

        self.execute(query, data.values())

    def where(self, query):
        select_query = "SELECT {} FROM {} WHERE {}".format(self.select_keys, self.table_name, query)
        result = self.execute(select_query)
        return result
