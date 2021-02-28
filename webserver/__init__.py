import os
import sys

from flask.config import Config

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
basepath = os.path.dirname(os.path.realpath(__file__))

from flask import Flask
from flask_sqlalchemy import SQLAlchemy
import logging
import logging.handlers as handlers

# Create app with changed paths  https://stackoverflow.com/a/42791810
app = Flask(
    __name__,
    instance_relative_config=True,
    template_folder="website/dist",
    static_folder="website/dist",
    static_url_path="",
)

logHandler = handlers.TimedRotatingFileHandler(
    basepath + "/logs/website.log", when="midnight", backupCount=100
)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
logHandler.setFormatter(formatter)
logHandler.setLevel(logging.INFO)
app.logger.addHandler(logHandler)
app.logger.setLevel(logging.INFO)

# Print our cool logo
from helpers.fancy_print_tcp import TCP_SIGN

app.logger.info("Loading config...")
if os.path.isfile("/mnt/config/config.py"):
    sys.path.append("/mnt/config/")
import config

# app.config.from_mapping(config)

from database.engine import DB_CONNECT_STRING

app.config["SQLALCHEMY_DATABASE_URI"] = DB_CONNECT_STRING

# I don't know what we have to call again to update the loggin level
# so let's just call everything...
logHandler.setLevel(logging.DEBUG if app.debug else logging.INFO)
app.logger.addHandler(logHandler)
app.logger.setLevel(logging.DEBUG if app.debug else logging.INFO)

app.logger.info("Done")

app.logger.info("Initializing streckennetz...")
from helpers.StreckennetzSteffi import StreckennetzSteffi

streckennetz = StreckennetzSteffi(prefer_cache=True)
app.logger.info("Done")

app.logger.info("Initializing AI...")
from webserver.predictor import Predictor

pred = Predictor()
app.logger.info("Done")

app.logger.info("DB init...")
db = SQLAlchemy()
db.init_app(app)
db.create_all(app=app)
app.logger.info("Done")

# ensure the instance folder exists
try:
    os.makedirs(app.instance_path)
except OSError:
    pass

from webserver.index import index_blueprint

app.register_blueprint(index_blueprint)

app.logger.info("Statistic init...")
from data_analysis.per_station import PerStationOverTime

per_station_time = PerStationOverTime(None, use_cache=True, logger=app.logger)
app.logger.info("Done")

app.logger.info("Initializing the api...")
from webserver import api

app.register_blueprint(api.bp)
app.logger.info("Done")

app.logger.info(
    "\nSetup done, webserver is up and running!\
    \n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n\n"
)
