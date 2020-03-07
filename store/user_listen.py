from .data_store import DataStore

class UserListen(DataStore):
    create_table_query = """
        CREATE TABLE IF NOT EXISTS {}
        (
            song          text,
            firstName     text,
            lastName      text,
            PRIMARY KEY   (song)
        )
    """

    select_keys = 'firstName, lastName'
