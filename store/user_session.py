from .data_store import DataStore

class UserSession(DataStore):
    table_name = "user_sessions"
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
    """.format(table_name)

    select_keys = 'artist, song, firstName, lastName'
