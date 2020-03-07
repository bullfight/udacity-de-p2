
# Part 1: Extract and Transform Event Data

**import packages**


```python
from data_store import DataStore
from extract import Extract
import pandas
```

Use the Extract class extract and tranform event_data files

* Find all datafile
* Combine into a single dataframe with pandas
* Drop nas on the 'artist' column
* Export to CSV


```python
extract = Extract('./event_data')
columns = ['artist','firstName','gender','itemInSession','lastName','length', 'level','location','sessionId','song','userId']
dropna  = ['artist']
extract.save('event_datafile_new.csv', columns, dropna)
```

Let's take a peek at the data to make sure it looks right


```python
data_frame = pandas.read_csv('event_datafile_new.csv')
data_frame.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>artist</th>
      <th>firstName</th>
      <th>gender</th>
      <th>itemInSession</th>
      <th>lastName</th>
      <th>length</th>
      <th>level</th>
      <th>location</th>
      <th>sessionId</th>
      <th>song</th>
      <th>userId</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Stephen Lynch</td>
      <td>Jayden</td>
      <td>M</td>
      <td>0</td>
      <td>Bell</td>
      <td>182.85669</td>
      <td>free</td>
      <td>Dallas-Fort Worth-Arlington, TX</td>
      <td>829</td>
      <td>Jim Henson's Dead</td>
      <td>91</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Manowar</td>
      <td>Jacob</td>
      <td>M</td>
      <td>0</td>
      <td>Klein</td>
      <td>247.56200</td>
      <td>paid</td>
      <td>Tampa-St. Petersburg-Clearwater, FL</td>
      <td>1049</td>
      <td>Shell Shock</td>
      <td>73</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Morcheeba</td>
      <td>Jacob</td>
      <td>M</td>
      <td>1</td>
      <td>Klein</td>
      <td>257.41016</td>
      <td>paid</td>
      <td>Tampa-St. Petersburg-Clearwater, FL</td>
      <td>1049</td>
      <td>Women Lose Weight (Feat: Slick Rick)</td>
      <td>73</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Maroon 5</td>
      <td>Jacob</td>
      <td>M</td>
      <td>2</td>
      <td>Klein</td>
      <td>231.23546</td>
      <td>paid</td>
      <td>Tampa-St. Petersburg-Clearwater, FL</td>
      <td>1049</td>
      <td>Won't Go Home Without You</td>
      <td>73</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Train</td>
      <td>Jacob</td>
      <td>M</td>
      <td>3</td>
      <td>Klein</td>
      <td>216.76363</td>
      <td>paid</td>
      <td>Tampa-St. Petersburg-Clearwater, FL</td>
      <td>1049</td>
      <td>Hey_ Soul Sister</td>
      <td>73</td>
    </tr>
  </tbody>
</table>
</div>



# Part 2: Load and Query the Extracted Data

### Query 1:  Give me the artist, song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession = 4

Start by defining the Session class to model user listening sessions

The primary keys for this model are: 

```sql
    PRIMARY KEY (sessionId, itemInSession)
```

These keys ensure that:

* we can query first by `sessionId` and `itemInSession`.
* `sessionId` is unqiue to a user.
* `itemInSession` when combine with session ensures uniqueness within a session.


```python
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
```

Now use the model to load and query event data


```python
session = Session()
session.load('./event_datafile_new.csv')
```


```python
session.where('sessionid = 338 AND itemInSession = 4')
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>artist</th>
      <th>song</th>
      <th>length</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Faithless</td>
      <td>Music Matters (Mark Knight Dub)</td>
      <td>495.3073</td>
    </tr>
  </tbody>
</table>
</div>



### Query 2: Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182

Start by defining the User Session class to model artists listened to in a session

The primary keys for this model are: 

```sql
    PRIMARY KEY (userId, sessionId, itemInSession)
```

These keys ensure that:

* we can query first by `userId`, and then `sessionId`.
* `userId` provides for the unique user when quering for user session listens.
* `itemInSession` is added to providing sorting to the resultant output.


```python
class UserSession(DataStore):
    primary_keys = ['userId', 'sessionId', 'itemInSession']
    select_keys  = ['artist', 'song', 'firstName', 'lastName']
    columns      = {
        'userId':        'int',
        'sessionId':     'int',
        'itemInSession': 'int',
        'artist':        'text',
        'song':          'text',
        'firstName':     'text',
        'lastName':      'text',
    }
```


```python
user_session = UserSession()
user_session.load('./event_datafile_new.csv')
user_session.where('userId = 10 AND sessionId = 182')
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>artist</th>
      <th>song</th>
      <th>firstname</th>
      <th>lastname</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Down To The Bone</td>
      <td>Keep On Keepin' On</td>
      <td>Sylvie</td>
      <td>Cruz</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Three Drives</td>
      <td>Greece 2000</td>
      <td>Sylvie</td>
      <td>Cruz</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Sebastien Tellier</td>
      <td>Kilometer</td>
      <td>Sylvie</td>
      <td>Cruz</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Lonnie Gordon</td>
      <td>Catch You Baby (Steve Pitron &amp; Max Sanna Radio...</td>
      <td>Sylvie</td>
      <td>Cruz</td>
    </tr>
  </tbody>
</table>
</div>



### Query 3: Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'

Start by defining the User Listens class to model songs listened to by users

The primary keys for this model are: 

```sql
    PRIMARY KEY (song, userId)
```

These keys ensure that:

* we can query by `song`.
* `userId` provides for the uniquness of the user when quering for listens.


```python
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
```


```python
user_listen = UserListen()
user_listen.load('./event_datafile_new.csv')
user_listen.where("song = 'All Hands Against His Own'")
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>firstname</th>
      <th>lastname</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Jacqueline</td>
      <td>Lynch</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Tegan</td>
      <td>Levine</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Sara</td>
      <td>Johnson</td>
    </tr>
  </tbody>
</table>
</div>



## Drop the tables before closing out the sessions


```python
session.drop_table()
user_session.drop_table()
user_listen.drop_table()
```

## Close the session and cluster connection


```python
session.shutdown()
user_session.shutdown()
user_listen.shutdown()
```
