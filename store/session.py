from .data_store import DataStore

class Session(DataStore):
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
    """

    select_keys = 'artist, song, length'
