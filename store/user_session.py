from .data_store import DataStore

class UserSession(DataStore):
    primary_keys = ['userId', 'sessionId', 'itemInSession']
    select_keys  = ['artist', 'song', 'firstName', 'lastName']
    attributes = {
        'userId':        'int',
        'sessionId':     'int',
        'itemInSession': 'int',
        'artist':        'text',
        'song':          'text',
        'firstName':     'text',
        'lastName':      'text',
    }
