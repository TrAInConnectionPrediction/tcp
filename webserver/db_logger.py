import datetime
from flask_sqlalchemy import SQLAlchemy
# from webserver import db
from flask import json, request
from functools import wraps
from sqlalchemy.dialects.postgresql import JSON

db = SQLAlchemy()

class LogEntry(db.Model):
    __tablename__ = 'website_connect_log'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)

    platform = db.Column(db.String)
    browser = db.Column(db.String)
    version = db.Column(db.String)
    user_agent = db.Column(db.String)
    page = db.Column(db.String)
    host_url = db.Column(db.String)
    time = db.Column(db.DateTime)
    ip = db.Column(db.String)
    args = db.Column(JSON)
    kwargs = db.Column(JSON)
    request_data = db.Column(JSON)

    def __init__(self, page, host_url, platform, browser, version, user_agent, ip, request_data, args, kwargs):
        self.time = datetime.datetime.now()
        self.page = page
        self.host_url = host_url
        self.platform = platform
        self.browser = browser
        self.version = version
        self.user_agent = user_agent
        self.ip = ip
        self.request_data = request_data
        self.args = args
        self.kwargs = kwargs


def log_activity(func):
    """
    A decorator, that logs all the traffic through this function to the Database
    """
    @wraps(func)
    def decorated(*args, **kwargs):
        db.session.add(
            LogEntry(
                page=request.path,
                host_url=request.host_url,
                platform=request.user_agent.platform,
                browser=request.user_agent.browser,
                version=request.user_agent.version,
                user_agent=request.user_agent.string,
                ip=request.remote_addr if 'X-Forwarded-For' not in request.headers else request.headers['X-Forwarded-For'],
                request_data=request.json,
                args=args,
                kwargs=kwargs
            )
        )
        db.session.commit()

        return func(*args, **kwargs)
    return decorated