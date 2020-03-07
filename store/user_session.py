from .data_store import DataStore

class UserSession(DataStore):
    create_table_query = """
        CREATE TABLE IF NOT EXISTS {}
        (
            userId        int,
            sessionId     int,
            itemInSession int,
            artist        text,
            song          text,
            firstName     text,
            lastName      text,
            PRIMARY KEY   (userId, sessionId, itemInSession)
        )
    """

    select_keys = 'artist, song, firstName, lastName'
