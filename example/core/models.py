from task_manager.db import TaskMixin, TaskLogMixin
from sqlalchemy.ext.declarative import declarative_base
from flask_sqlalchemy import SQLAlchemy, BaseQuery, _QueryProperty

Base = declarative_base()
db = SQLAlchemy()
db.Model = Base
db.Model.query_class = BaseQuery
db.Model.query = _QueryProperty(db)


class User(db.Model):
    __tablename__ = 'user'
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    email = db.Column(db.String(120), unique=True, nullable=False)


class Task(Base, TaskMixin):
    pass


class TaskLog(Base, TaskLogMixin):
    pass
