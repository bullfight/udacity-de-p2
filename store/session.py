from .data_store import DataStore

class Session(DataStore):
    table_name = "sessions"
    create_table_query = """
        CREATE TABLE IF NOT EXISTS {}
        (
            sessionId     int,
            itemInSession int,
            artist        text,
            song          text,
            length        float,
            PRIMARY KEY  (sessionId, itemInSession)
        )
    """.format(table_name)

    select_keys = 'artist, song, length'
