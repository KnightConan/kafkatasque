from flask_wtf.csrf import CSRFProtect
from flask import (
    Blueprint, render_template, redirect, url_for
)
from .models import Task, TaskLog
from .tasks import test_task
from .config import DevelopmentConfig
from task_manager.queue import Queue
from task_manager.db import TaskManager
from task_manager.schedule import TaskScheduler
from kafka import KafkaProducer
from random import randint
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

some_engine = create_engine(DevelopmentConfig.SQLALCHEMY_DATABASE_URI)
Session = sessionmaker(bind=some_engine)

# csrf protection
csrf = CSRFProtect()

tm = TaskManager(Task, TaskLog, Session())
producer = KafkaProducer(bootstrap_servers='127.0.0.1:9092')
queue = Queue(producer, task_manager=tm)
ts = TaskScheduler(queue)

# blueprint for news related views
news = Blueprint('news', __name__, template_folder='templates')


@news.route('/', methods=['GET'])
def overview():
    tasks = Task.query.order_by(Task.id.asc()).all()
    return render_template('overview.html', tasks=tasks)


@news.route('/new_task/', methods=['GET'])
def new_task():
    queue.enqueue("example_topic", randint(10, 40), func=test_task, use_db=True)
    return redirect(url_for('news.overview'))


@news.route('/new_simple_task/', methods=['GET'])
def new_simple_task():
    queue.enqueue("example_topic", randint(10, 40), use_db=True)
    return redirect(url_for('news.overview'))


@news.route('/postpone_task/<int:task_id>/', methods=['GET'])
def postpone_task(task_id):
    ts.postpone_task(task_id, 1)
    return redirect(url_for('news.overview'))


@news.route('/cancel_task/<int:task_id>/', methods=['GET'])
def cancel_task(task_id):
    ts.cancel_task(task_id, 1)
    return redirect(url_for('news.overview'))


@news.route('/repeat_task/<int:task_id>/', methods=['GET'])
def repeat_task(task_id):
    ts.repeat_task(task_id, 1)
    return redirect(url_for('news.overview'))
