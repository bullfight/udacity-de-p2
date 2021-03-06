from .data_store import DataStore

class UserListen(DataStore):
    primary_keys = ['song', 'userId']
    select_keys  = ['firstName', 'lastName']
    columns      = {
        'song':      'text',
        'userId':    'int',
        'artist':    'text',
        'firstName': 'text',
        'lastName':  'text'
    }
