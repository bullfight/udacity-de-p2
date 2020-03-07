from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import pandas

class DataStore:
    def __init__(self):
        self.session = self.__setup_session()
        self.__setup_keyspace()

    def create_table(self):
        query = """
            CREATE TABLE IF NOT EXISTS {} (
                {},
                PRIMARY KEY  ({})
            )
        """.format(self.__build_table_name(), self.__build_columns(), self.__build_primary_keys())

        self.__execute(query)

    def drop_table(self):
        query = "DROP TABLE IF EXISTS {}".format(self.__build_table_name())
        self.__execute(query)

    def load(self, filepath):
        self.drop_table()
        self.create_table()
        data_frame = pandas.read_csv(filepath)
        columns    = list(self.attributes.keys())

        for i, row in data_frame.iterrows():
            data = dict(row[columns])
            self.create(data)

    def create(self, data):
        keys = list(data.keys())
        names = ', '.join(keys)
        insert_string = ['%s'] * len(keys)
        insert_string = ', '.join(insert_string)
        query = "INSERT INTO {} ({}) VALUES ({})".format(self.__build_table_name(), names, insert_string)

        self.__execute(query, data.values())

    def where(self, query):
        select_query = "SELECT {} FROM {} WHERE {}".format(self.__build_select_keys(), self.__build_table_name(), query)
        result = self.__execute(select_query)
        return result

    def __setup_session(self):
        auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
        cluster = Cluster(auth_provider=auth_provider)
        session = cluster.connect()
        return session

    def __setup_keyspace(self):
        self.session.execute(
            """
                CREATE KEYSPACE IF NOT EXISTS udacity
                WITH REPLICATION =
                { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }
            """
        )

        self.session.set_keyspace('udacity')


    def __execute(self, sql, data=None):
        result = self.session.execute(sql, data)
        return result

    def __build_table_name(self):
        name = self.__class__.__name__
        name = name.lower() + 's'

        return name

    def __build_columns(self):
        output = []
        for key, value in self.attributes.items():
            value = "{} {}".format(key, value)
            output.append(value)

        columns = ', '.join(output)

        return columns

    def __build_primary_keys(self):
        return ', '.join(self.primary_keys)

    def __build_select_keys(self):
        return ', '.join(self.select_keys)
