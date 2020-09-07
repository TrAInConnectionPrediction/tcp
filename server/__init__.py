import os

from flask import Flask, render_template
import logging
import logging.handlers as handlers


def create_app(test_config=None):
    """Create and configure an instance of the Flask application."""
    #we need to change the paths  https://stackoverflow.com/a/42791810
    app = Flask(__name__, instance_relative_config=True, template_folder='website',static_folder='website/',static_url_path='')
    app.config.from_mapping(
        # a default secret that should be overridden by instance config
        SECRET_KEY="dev",
    )

    if test_config is None:
        # load the instance config, if it exists, when not testing
        app.config.from_pyfile("../config.py", silent=True)
    else:
        # load the test config if passed in
        app.config.update(test_config)

    # ensure the instance folder exists
    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass

    basepath = os.path.dirname(os.path.realpath(__file__))
    logHandler = handlers.TimedRotatingFileHandler(basepath + '/logs/website.log', when='midnight', backupCount=100)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    logHandler.setFormatter(formatter)
    logHandler.setLevel(logging.DEBUG if app.debug else logging.INFO)
    app.logger.addHandler(logHandler)
    app.logger.setLevel(logging.DEBUG if app.debug else logging.INFO)
    @app.route("/")
    def home(output = []):
        """
        Gets called when somebody requests the website
        If we want we can redirect to kepiserver.de to the main server

        Args:
            -

        Returns:
            html page: the html homepage
        """
        return render_template('index.html')

    @app.errorhandler(404)
    def not_found(e): 
        # inbuilt function which takes error as parameter 
        # defining function 
        return render_template("404.html")

    from server import api

    app.register_blueprint(api.bp)

    return app

