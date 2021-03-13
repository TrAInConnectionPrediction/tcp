from flask import Blueprint, render_template

index_blueprint = Blueprint("index", __name__, url_prefix="")


@index_blueprint.route("/")
@index_blueprint.route("/about")
@index_blueprint.route("/stats")
@index_blueprint.route("/impressum")
def home(output=[]):
    """
    Gets called when somebody requests the website

    Args:
        -

    Returns:
        webpage: the home-/landing page
    """
    return render_template('index.html')

# @index_blueprint.app_errorhandler(404)
# def not_found(e):
#     """
#     Custom 404 Page
#     Get's called if the page can not be found.

#     Args:
#         -

#     Returns:
#         webpage: our custom 404-page
#     """
#     # inbuilt function which takes error as parameter
#     # defining function
#     return render_template("404.html")