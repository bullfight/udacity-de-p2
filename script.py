import store

extract = store.Extract('./event_data')
columns = ['artist','firstName','gender','itemInSession','lastName','length', 'level','location','sessionId','song','userId']
dropna  = ['artist']
extract.save('event_datafile_new.csv', columns, dropna)

print('Session')
session = store.Session()
#session.load('./event_datafile_new.csv')
results = session.where('sessionid = 338 AND itemInSession = 4')
print(results)

print('User Session')
user_session = store.UserSession()
user_session.load('./event_datafile_new.csv')
results = user_session.where('userId = 10 AND sessionId = 182')
print(results)

print('User Listens')
user_listen = store.UserListen()
user_listen.load('./event_datafile_new.csv')
results = user_listen.where("song = 'All Hands Against His Own'")
print(results)
