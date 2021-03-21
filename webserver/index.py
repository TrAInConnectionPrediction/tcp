from flask import Blueprint, render_template

index_blueprint = Blueprint("index", __name__, url_prefix="")

# This routes everything to vue,
# but for nested stuff you have to add a seperate route
@index_blueprint.route("/", defaults={"path": ""})
@index_blueprint.route("/<string:path>")
@index_blueprint.route("/data/<string:path>")
def home(**kwargs):
    """
    Gets called when somebody requests the website

    Args:
        -

    Returns:
        webpage: the home-/landing page
    """
    return render_template("index.html")


@index_blueprint.app_errorhandler(404)
def not_found(e):
    """
    Custom 404 Page
    Get's called if the page can not be found.

    Args:
        -

    Returns:
        webpage: our custom 404-page
    """
    return render_template("404.html")