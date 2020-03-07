from .data_store import DataStore

class UserListen(DataStore):
    primary_keys = ['song']
    select_keys  = ['firstName', 'lastName']
    attributes   = {
        'song':      'text',
        'firstName': 'text',
        'lastName':  'text'
    }


