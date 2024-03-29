import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from config import db_database, db_password, db_server, db_username

DB_CONNECT_STRING = 'postgresql+psycopg2://' + db_username + ':' + db_password + '@' + db_server + '/' + db_database + '?sslmode=require'

def get_engine():
    return sqlalchemy.create_engine(
        DB_CONNECT_STRING,
        pool_pre_ping=True,
        pool_recycle=3600,
        # future=True,
        pool_size=1,
        max_overflow=0,
    )

# Session = sessionmaker(bind=get_engine())
def sessionfactory():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    return engine, Session
# Session = None


from contextlib import contextmanager

@contextmanager
def session_scope(Session):
    """Provide a transactional scope around a series of operations."""
    session = Session()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()
