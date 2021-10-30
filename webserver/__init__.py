import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
basepath = os.path.dirname(os.path.realpath(__file__))
if os.path.isfile("/mnt/config/config.py"):
    sys.path.append("/mnt/config/")
import config

from flask import Flask
import pyhafas
from helpers import StreckennetzSteffi, logging
from data_analysis.per_station import PerStationOverTime
from webserver.index import index_blueprint
from webserver.db_logger import db

# Do not use GUI for matplotlib
import matplotlib
matplotlib.use('Agg')

client = pyhafas.HafasClient(pyhafas.profile.DBProfile())

logging.info('Initialising streckennetz')
streckennetz = StreckennetzSteffi(prefer_cache=False)
logging.info('Done!')

logging.info('Initialising per_station_time')
per_station_time = PerStationOverTime(None, prefer_cache=False)
logging.info('Done!')

from webserver.predictor import Predictor
logging.info('Initialising predictior')
predictor = Predictor(n_models=15)
logging.info('Done!')


def create_app():
    import helpers.fancy_print_tcp

    # Create app with changed paths  https://stackoverflow.com/a/42791810
    app = Flask(
        __name__,
        instance_relative_config=True,
        template_folder="website/dist",
        static_folder="website/dist",
        static_url_path="",
    )

    from webserverconfig import ProductionConfig, DevelopmentConfig
    if app.config["ENV"] == "production":
        app.config.from_object(ProductionConfig)
    else:
        app.config.from_object(DevelopmentConfig)

    app.logger.info("DB init...")
    db.init_app(app)
    db.create_all(app=app)
    app.logger.info("Done")

    # ensure the instance folder exists
    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass

    app.register_blueprint(index_blueprint)

    app.logger.info("Initializing the api...")
    from webserver import api
    app.register_blueprint(api.bp)
    app.logger.info("Done")

    app.logger.info(
        "\nSetup done, webserver is up and running!\
        \n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n\n"
    )

    return app