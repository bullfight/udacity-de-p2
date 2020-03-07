from .data_store import DataStore

class Session(DataStore):
    primary_keys = ['sessionId', 'itemInSession']
    select_keys  = ['artist', 'song', 'length']
    columns      = {
        'sessionId':     'int',
        'itemInSession': 'int',
        'artist':        'text',
        'song':          'text',
        'length':        'double'
    }
