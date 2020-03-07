import store

print('Session')
session = store.Session()
session.load('./event_datafile_new.csv')
results = session.where('sessionid = 338 AND itemInSession = 4')
for row in results :
    print(row)

print('User Session')
user_session = store.UserSession()
user_session.load('./event_datafile_new.csv')
results = user_session.where('userId = 10 AND sessionId = 182')
for row in results :
    print(row)

print('User Listens')
user_listen = store.UserListen()
user_listen.load('./event_datafile_new.csv')
results = user_listen.where("song = 'All Hands Against His Own'")
for row in results :
    print(row)
