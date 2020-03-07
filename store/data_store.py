from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import pandas

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

    def table_name(self):
        name = self.__class__.__name__
        name = name.lower() + 's'

        return name

    def build_columns(self):
        output = []
        for key, value in self.attributes.items() :
            value = "{} {}".format(key, value)
            output.append(value)

        columns = ', '.join(output)

        return columns

    def build_primary_keys(self):
        return ', '.join(self.primary_keys)

    def create_table(self):
        query = """
            CREATE TABLE IF NOT EXISTS {} (
                {},
                PRIMARY KEY  ({})
            )
        """.format(self.table_name(), self.build_columns(), self.build_primary_keys())

        self.execute(query)

    def drop_table(self):
        query = "DROP TABLE IF EXISTS {}".format(self.table_name())
        self.execute(query)

    def create(self, data):
        keys = list(data.keys())
        names = ', '.join(keys)
        insert_string = ['%s'] * len(keys)
        insert_string = ', '.join(insert_string)
        query = "INSERT INTO {} ({}) VALUES ({})".format(self.table_name(), names, insert_string)

        self.execute(query, data.values())

    def build_select_keys(self):
        return ', '.join(self.select_keys)

    def where(self, query):
        select_query = "SELECT {} FROM {} WHERE {}".format(self.build_select_keys(), self.table_name(), query)
        result = self.execute(select_query)
        return result

    def load(self, filepath):
        self.drop_table()
        self.create_table()
        data_frame = pandas.read_csv(filepath)
        columns    = list(self.attributes.keys())

        for i, row in data_frame.iterrows():
            data = dict(row[columns])
            self.create(data)
