from flask_script import Server, Manager
from flask_migrate import MigrateCommand
from core import create_app


app = create_app("core.config.DevelopmentConfig")
manager = Manager(app)
manager.add_command("runserver", Server(port=8050))
manager.add_command('db', MigrateCommand)


if __name__ == "__main__":
    manager.run()
