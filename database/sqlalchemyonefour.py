def _gt14() -> bool:
    """
    Check if sqlalchemy.__version__ is at least 1.4.0, when several
    deprecations were made.
    """
    import sqlalchemy

    return '1.4.' in sqlalchemy.__version__