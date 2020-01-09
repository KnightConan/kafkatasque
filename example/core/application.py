import logging

from flask import Flask
from flask_migrate import Migrate


def create_app(config_obj_str):
    """
    Function to create the app instance and apply all the configurations to it

    Args:
        config_obj_str (str): path to the config object.

    Returns:
        app instance: the flask app instance for the project.
    """
    app = Flask(__name__)

    # load configurations
    app.config.from_object(config_obj_str)

    # logging config
    loglevel = logging.INFO
    log_format = logging.Formatter(
        '%(asctime)s %(levelname)s ehde-labeling-app %(message)s'
    )

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(log_format)
    stream_handler.setLevel(loglevel)

    app.logger.addHandler(stream_handler)
    app.logger.setLevel(loglevel)

    # initial db
    from .models import db
    db.init_app(app)

    from .views import news
    app.register_blueprint(news)

    # initial login_manager
    from .views import csrf
    csrf.init_app(app)

    migrate = Migrate(app, db)

    return app
