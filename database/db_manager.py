from database.plan import Plan
from database.change import Change
from database.rtd import Rtd, sql_types
from database.engine import get_engine
from sqlalchemy.orm import sessionmaker
import datetime
import pangres
import pandas as pd


class DBManager:
    def __init__(self) -> None:
        self.Plan = Plan
        self.Change = Change
        self.Rtd = Rtd

    def __enter__(self):
        self.engine = get_engine()
        self.session = sessionmaker(bind=self.engine)()
        return self
    
    def __exit__(self, exc_type, exc_value, exc_traceback): 
        self.session.close()
        self.engine.dispose()


    def plan_of_station(self, station: str, date1: datetime.datetime, date2: datetime.datetime):
        """Return planed timetables for station

        Parameters
        ----------
        station : str
            Station name
        date1 : datetime.datetime or None
            Lower limit, inclusive
        date2 : datetime.datetime or None
            Upper limit, exclusive

        Returns
        -------
        list
            Planned timetable of station between date1 and date2
        """
        if date1 is None:
            return self.session.query(self.Plan).filter((self.Plan.bhf == station)).all()
        if date2 is None:
            return self.session.query(self.Plan).filter(
                (self.Plan.bhf == station) & (self.Plan.date == date1)).first()
        return self.session.query(self.Plan).filter((self.Plan.bhf == station)
                                               & (self.Plan.date >= date1)
                                               & (self.Plan.date < date2)).all()

    def count_plan_entries_at_date(self, date: datetime.datetime) -> int:
        return self.session.query(self.Plan).filter(self.Plan.date == date).count()

    def get_changes(self, hash_ids: list):
        """
        Get changes that have a given hash_id

        Parameters
        ----------
        hash_ids: list
            A list of hash_ids to get the corresponding rows from the db.

        Returns
        -------
        Sqlalchemy query with the results.
        """
        return self.session.query(self.Change).filter(self.Change.hash_id.in_(hash_ids)).all()

    def count_change_entries(self) -> int:
        """
        Get the number of rows in db.

        Returns
        -------
        int
            Number of Rows.
        """
        return self.session.query(self.Change).count()

    @staticmethod
    def upsert_rtd(rtd: pd.DataFrame):
        """
        Upsert dataframe to db using pangres

        Parameters
        ----------
        rtd: pd.DataFrame
            Data to upsert
        """
        if not rtd.empty:
            engine = get_engine()
            pangres.upsert(engine,
                        rtd,
                        if_row_exists='update',
                        table_name=Rtd.__tablename__,
                        dtype=sql_types,
                        create_schema=False,
                        add_new_columns=False,
                        adapt_dtype_of_empty_db_columns=False)
            engine.dispose()