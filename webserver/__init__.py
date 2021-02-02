import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from flask import Flask
from flask_sqlalchemy import SQLAlchemy
import logging
import logging.handlers as handlers

from helpers.StreckennetzSteffi import StreckennetzSteffi
from database.engine import DB_CONNECT_STRING

basepath = os.path.dirname(os.path.realpath(__file__))

streckennetz = StreckennetzSteffi(prefer_cache=True)
from webserver.predictor import Predictor
pred = Predictor()
db = SQLAlchemy()


def create_app():
    #Print our cool logo
    from helpers.fancy_print_tcp import TCP_SIGN

    # Create app with changed paths  https://stackoverflow.com/a/42791810
    app = Flask(__name__, instance_relative_config=True,
            template_folder='website', static_folder='website/', static_url_path='')

    app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0
    app.config['SQLALCHEMY_DATABASE_URI'] = DB_CONNECT_STRING

    db.init_app(app)

    logHandler = handlers.TimedRotatingFileHandler(basepath + '/logs/website.log', when='midnight', backupCount=100)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    logHandler.setFormatter(formatter)
    logHandler.setLevel(logging.INFO)
    app.logger.addHandler(logHandler)
    app.logger.setLevel(logging.INFO)

    logHandler.setLevel(logging.DEBUG if app.debug else logging.INFO)
    app.logger.addHandler(logHandler)
    app.logger.setLevel(logging.DEBUG if app.debug else logging.INFO)
    app.logger.info("Done")

    # ensure the instance folder exists
    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass

    app.logger.info("Initializing the api...")
    from webserver import api
    app.register_blueprint(api.bp)
    app.logger.info("Done")

    from webserver.index import index_blueprint
    app.register_blueprint(index_blueprint)

    db.create_all(app=app)

    app.logger.info("Setup done... starting webserver")
    app.logger.info(
        "\nSetup done, webserver is up and running!\
        \n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n\n")

    return app