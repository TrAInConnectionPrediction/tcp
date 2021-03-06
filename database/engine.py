import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import sqlalchemy
from config import db_database, db_password, db_server, db_username


DB_CONNECT_STRING = 'postgresql://' + db_username + ':' + db_password + '@' + db_server + '/' + db_database + '?sslmode=require'

engine = sqlalchemy.create_engine(
    DB_CONNECT_STRING,
    pool_pre_ping=True,
    pool_recycle=3600
)
