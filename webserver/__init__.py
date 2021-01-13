import os

from flask import Flask, render_template
import logging
import logging.handlers as handlers


basepath = os.path.dirname(os.path.realpath(__file__))


# we need to change the paths  https://stackoverflow.com/a/42791810
app = Flask(__name__, instance_relative_config=True,
            template_folder='website', static_folder='website/', static_url_path='')


logHandler = handlers.TimedRotatingFileHandler(basepath + '/logs/website.log', when='midnight', backupCount=100)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
logHandler.setFormatter(formatter)
logHandler.setLevel(logging.INFO)
app.logger.addHandler(logHandler)
app.logger.setLevel(logging.INFO)
logger = app.logger

#Print our cool logo
from helpers.fancy_print_tcp import TCP_SIGN

app.logger.info("Loading config...")
from config import config
app.config.from_mapping(config)
#I don't know what we have to call again to update the loggin level
#so let's just call everything...
logHandler.setLevel(logging.DEBUG if app.debug else logging.INFO)
app.logger.addHandler(logHandler)
app.logger.setLevel(logging.DEBUG if app.debug else logging.INFO)
logger = app.logger
app.logger.info("Done")

app.logger.info("Initializing the streckennetz...")
from helpers.StreckennetzSteffi import StreckennetzSteffi
streckennetz = StreckennetzSteffi()
app.logger.info("Done")


app.logger.info("Initializing the machine learning...")
from webserver.predictor import Predictor
pred = Predictor()
app.logger.info("Done")


# ensure the instance folder exists
try:
    os.makedirs(app.instance_path)
except OSError:
    pass


@app.route("/")
def home(output=[]):
    """
    Gets called when somebody requests the website

    Args:
        -

    Returns:
        webpage: the home-/landing page
    """
    return render_template('index.html')

@app.errorhandler(404)
def not_found(e):
    """
    Custom 404 Page
    Get's called if the page can not be found.

    Args:
        -

    Returns:
        webpage: our custom 404-page
    """
    # inbuilt function which takes error as parameter
    # defining function
    return render_template("404.html")


app.logger.info("Initializing the api...")
from webserver import api
app.register_blueprint(api.bp)
app.logger.info("Done")


app.logger.info("Setup done... starting webserver")
app.logger.info(
    "\nSetup done, webserver is up and running!\
    \n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n\n")


def create_app_with_config(test_config):
    """Function for debug and testing"""
    if not test_config is None:
        app.config.from_mapping(test_config)
    return app
