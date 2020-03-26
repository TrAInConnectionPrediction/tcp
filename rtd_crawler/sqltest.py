import sqlalchemy
import pandas as pd

# TODO add config.py data
server = ''
database = ''
username = ''
password =  ''
engine = sqlalchemy.create_engine('postgresql://'+ username +':' + password + '@' + server + '/' + database + '?sslmode=require') #

df = pd.DataFrame({'name' : ['User 1', 'User 2', 'User 3']})
df.to_sql('users', if_exists='replace',  con=engine)