from sqlalchemy.dialects.postgresql import insert
import sqlalchemy
from typing import List

def upsert_base(
    session: sqlalchemy.orm.Session,
    table: sqlalchemy.sql.schema.Table,
    rows: List[dict]) -> None:
    """Upsert rows to table using session

    Parameters
    ----------
    session : sqlalchemy.orm.Session
        A session used to execute the upsert
    table : sqlalchemy.sql.schema.Table
        The table to upsert the rows to
    rows : List[dict]
        The actual data to upsert
    """    
    stmt = insert(table).values(rows)

    update_cols = [c.name for c in table.c
                    if c not in list(table.primary_key.columns)]

    on_conflict_stmt = stmt.on_conflict_do_update(
        index_elements=table.primary_key.columns,
        set_={k: getattr(stmt.excluded, k) for k in update_cols}
    )

    session.execute(on_conflict_stmt)


def do_nothing_upsert_base(
    session: sqlalchemy.orm.Session,
    table: sqlalchemy.sql.schema.Table,
    rows: List[dict]) -> None:
    """Upsert rows to table using session with do nothing if exists logic

    Parameters
    ----------
    session : sqlalchemy.orm.Session
        A session used to execute the upsert
    table : sqlalchemy.sql.schema.Table
        The table to upsert the rows to
    rows : List[dict]
        The actual data to upsert
    """    
    stmt = insert(table).values(rows)

    on_conflict_stmt = stmt.on_conflict_do_nothing(
        index_elements=table.primary_key.columns
    )

    session.execute(on_conflict_stmt)