from .data_store import DataStore

class UserListen(DataStore):
    table_name = "user_listens"
    create_table_query = """
        CREATE TABLE IF NOT EXISTS {}
        (
            song          text,
            firstName     text,
            lastName      text,
            PRIMARY KEY   (song)
        )
    """.format(table_name)

    select_keys = 'firstName, lastName'
