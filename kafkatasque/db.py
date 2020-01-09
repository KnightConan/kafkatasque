"""This module contains all classes related to database:
+ `TaskMixin` is a mixin class for `Task` model.
+ `TaskLogMixin` is the mixin class for `TaskLog` model.
+ `Tasker` is responsible for the database connection and executing
   queries.
"""

__all__ = ['JSONEncodedObj', 'TaskMixin', 'TaskLogMixin', 'Tasker']

import datetime
from typing import Optional, TypeVar, Type, Any

from sqlalchemy import (
    ForeignKey, Column, Integer, String, DateTime, Enum, TypeDecorator
)
from sqlalchemy.orm import relationship, validates
from sqlalchemy.orm.base import _is_mapped_class
from sqlalchemy.orm.session import make_transient, Session
from sqlalchemy.ext.declarative import declared_attr

from .constants import Status, Action, BLANK
from .utils import (
    json_encode, json_decode, validate_uuid4, validated_setter_property,
    session_required
)
from .job import TypeJob, Job


TypeTask = TypeVar('TypeTask', bound='TaskMixin')
TypeTaskLog = TypeVar('TypeTaskLog', bound='TaskLogMixin')


class JSONEncodedObj(TypeDecorator):
    """Column type for json encoded object, it uses `jsonpickle` package to
    encode the python object (including built-in types) while storing to DB, and
    correspondingly decoded while reading from DB. It's based on string type.
    """
    impl = String
    '''The class-level “impl” attribute is required, and can reference any 
    TypeEngine class. Here String is referenced as data type in DB.'''

    def process_bind_param(self,
                           value: Optional[str],
                           dialect) -> Optional[str]:
        """Encodes the input value to json format string.

        As a subclass, JSONEncodedObj overrides this method to return the
        value that should be passed along to the underlying
        :class:`.TypeEngine` object, and from there to the
        DBAPI ``execute()`` method.

        Dialect is not used because there is no need to behave differently for
        different DB.
        
        :param value: value to be encoded.
        :type value: str | None
        :param dialect: dislects defined in sqlalchemy.dialects
        :type dialect: sqlalchemy.dialects
        :return: encoded string
        :rtype: str | None
        """
        if value is not None:
            value = json_encode(value, to_bytes=False)
        return value

    def process_result_value(self,
                             value: Optional[str],
                             dialect) -> Any:
        """Decodes the json format string to python object.

        As a subclass, JSONEncodedObj overrides this method to return the
        value that should be passed back to the application,
        given a value that is already processed by
        the underlying :class:`.TypeEngine` object, originally
        from the DBAPI cursor method ``fetchone()`` or similar.

        Dialect is not used because there is no need to behave differently for
        different DB.

        :param value: value to be decoded.
        :type value: str| None
        :param dialect: dialects defined in sqlalchemy.dialects
        :type dialect: :doc:`sqlalchemy.dialects`
        :return: decoded python object
        :rtype: object
        """
        if value is not None:
            value = json_decode(value)
        return value


class SavableRecord:
    id = Column(Integer, primary_key=True)

    @session_required
    def save(self,
             session: Session,
             commit: bool = True) -> None:
        """Function to save a new record in DB, if commit is True; otherwise,
        just add the instance in the session.

        :param session: SQLAlchemy Session instance
        :type session: :doc:`sqlalchemy.orm.session.Session`
        :param commit: controls if commit the changes to DB or not.
        :type commit: bool
        """
        if self.id:
            raise TypeError("Save function is just for newly created record.")
        session.add(self)
        if commit:
            session.commit()


class TaskMixin(SavableRecord):
    """Mixin class for `Task` model, it contains all the necessary column
    definitions and methods. It requires the user to create a SQLAlchemy Task
    model inheriting from this mixin class.

    **Example:**

    .. code-block:: python

        from sqlalchemy.ext.declarative import declarative_base

        Base = declarative_base()

        class Task(Base, TaskMixin):
            pass
    """
    status = Column(Enum(Status), default=Status.waiting)
    """Task status, can be 'active', 'waiting', 'succeed', 'failed', 
    'postponed', 'cancelled', 'recovered'"""

    result = Column(String(200), nullable=False, default=BLANK)
    """The result of task function's execution, in form of string"""

    created_at = Column(DateTime, default=datetime.datetime.now)
    """Time of the task created"""

    # job columns
    job_id = Column(String(60), unique=True)
    """Id of job, should be in form of hex of uuid version 4"""

    job_topic = Column(String(120), nullable=False)
    """Kafka topic"""

    job_key = Column(JSONEncodedObj(120))
    """Kafka message key"""

    job_value = Column(JSONEncodedObj, nullable=False)
    """Kafka message value, it will be the first parameter passed to job_func"""

    job_partition = Column(Integer)
    """Kafka partition info"""

    job_func = Column(JSONEncodedObj)
    """Process function for the job_value"""

    job_args = Column(JSONEncodedObj)
    """Extra args for job_func"""

    job_kwargs = Column(JSONEncodedObj)
    """Extra kwargs for job_func"""

    job_timeout = Column(Integer, default=0)
    """Timeout threshold for executing the job_func"""

    @declared_attr
    def __tablename__(cls: Type[TypeTask]) -> str:
        """declared table name attribute for the subclass.

        This feature limits the subclass number of TaskMixin to 1. It's
        important to hard code the value of its subclass's table name, then
        the hook of TaskLogMixin can work.

        :return: table name for the subclass
        :rtype: str
        """
        return 'task'

    @validates('job_id')
    def validate_job_id(self,
                        key: str,
                        value: Optional[str]) -> Optional[str]:
        """Validates the given job_id value is a valid string of hex of uuid
        in version 4.

        :param key: key
        :type key: str
        :param value: given value of job_id
        :type value: str | None
        :return: value of job_id
        :rtype: str | None
        """
        if value is not None and not validate_uuid4(value):
            raise ValueError("Parameter 'job_id' is not a valid UUID.")
        return value

    @classmethod
    def create(cls: Type[TypeTask],
               job: TypeJob) -> Optional[TypeTask]:
        """Function to create a Task instance by passing a job instance.

        If use an instance of a customized Job class, this method has to be
        overridden.

        :param job: Job instance.
        :type job: Job
        :return: a Task instance
        :rtype: Task | None

        **Example:**

        .. code-block:: python

            import uuid
            from sqlalchemy.ext.declarative import declarative_base
            from sqlalchemy import create_engine
            from sqlalchemy.orm import sessionmaker
            from .job import Job

            some_engine = create_engine('your postgres url')

            Session = sessionmaker(bind=some_engine)

            Base = declarative_base()

            class Task(Base, TaskMixin):
                pass

            job = Job(topic="example", value="Hello World", use_db=True)
            task = Task.create(job)

        """
        if not isinstance(job, Job):
            raise TypeError("Parameter 'job' should be Job. Received "
                            "{}.".format(type(job)))
        return cls(job_id=job.id, job_topic=job.topic, job_key=job.key,
                   job_value=job.value, job_partition=job.partition,
                   job_func=job.func, job_args=job.args, job_kwargs=job.kwargs,
                   job_timeout=job.timeout)

    @session_required
    def update(self,
               session: Session,
               commit: bool = True,
               **kwargs: Any) -> None:
        """Update the columns' values of a Task instance.

        It's a Django style update function in Flask. The new values has to be
        passed as keyword parameters.

        :param session: SQLAlchemy Session instance
        :type session: :doc:`sqlalchemy.orm.session.Session`
        :param commit: controls if commit the changes to DB or not.
        :type commit: bool
        :param kwargs: keyword parameters
        """
        for attr, new_value in kwargs.items():
            setattr(self, attr, new_value)
        if commit:
            session.commit()

    @classmethod
    @session_required
    def duplicate(cls: Type[TypeTask],
                  session: Session,
                  task_id: int,
                  commit: bool = True,
                  **kwargs: Any) -> Optional[TypeTask]:
        """Copy the information from a task record to a new task record, then
        save it in DB.

        :param session: SQLAlchemy Session instance
        :type session: :doc:`sqlalchemy.orm.session.Session`
        :param task_id: id of task to duplicate
        :type task_id: int
        :param commit: controls if commit the changes to DB or not.
        :type commit: bool
        :param kwargs: keyword arguments to override the
        :return: new created Task instance
        :rtype: Task
        """
        if not isinstance(task_id, int):
            raise TypeError("Parameter 'task_id' must be int. "
                            "Received {}".format(type(task_id)))
        task = session.query(cls).filter_by(id=task_id).first()
        if not task:
            raise ValueError("Given 'task_id' not found in DB.")
        session.expunge(task)
        make_transient(task)

        attrs_to_override = dict(id=None, result=BLANK,
                                 status=Status.waiting, job_id=None)
        attrs_to_override.update(kwargs)
        for k, v in attrs_to_override.items():
            setattr(task, k, v)
        task.save(session, commit)
        return task


class TaskLogMixin(SavableRecord):
    """Mixin class for TaskLog model.

    TaskLog model should track all the important actions taken by user
    or system (recovery).
    """
    comment = Column(String(200), nullable=False, default=BLANK)
    """Free text for comment. It's also used for storing the repeated task's 
    id."""

    action = Column(Enum(Action))
    """Action has been taken to the task"""

    created_at = Column(DateTime, default=datetime.datetime.now)
    """Created timestamp"""

    @declared_attr
    def __tablename__(cls: Type[TypeTaskLog]) -> str:
        """declared table name attribute for the subclass.

        This feature limits the subclass number of TaskLogMixin to 1.

        :return: table name for the subclass
        :rtype: str
        """
        return 'task_log'

    @declared_attr
    def creator_id(cls: Type[TypeTaskLog]) -> Column:
        """ForeignKey hook for linking the TaskLog and User table together.

        :return: foreign key column for user table
        :rtype: Column
        """
        return Column(Integer, ForeignKey('user.id'))

    @declared_attr
    def creator(cls: Type[TypeTaskLog]) -> relationship:
        """Relationship hook for user table.

        :return: relationship with User table
        :rtype: relationship
        """
        return relationship("User")

    @declared_attr
    def task_id(cls: Type[TypeTaskLog]) -> Column:
        """ForeignKey hook for linking the TaskLog and Task table together.

        :return: foreign key column for task table
        :rtype: Column
        """
        return Column(Integer, ForeignKey('task.id'))

    @declared_attr
    def task(cls: Type[TypeTaskLog]) -> relationship:
        """Relationship hook for task table.

        :return: relationship with Task table
        :rtype: relationship
        """
        return relationship("Task", backref="log")


class Tasker:
    """Class to control the operations for Task and TaskLog models with the DB.

    :param task_model: Task model class
    :type task_model: Task model class
    :param task_log_model: TaskLog model class
    :type task_log_model: TaskLog model class
    :param session: SQLAlchemy Session instance
    :type session: :doc:`sqlalchemy.orm.session.Session`
    """
    def __init__(self,
                 task_model: Type[TypeTask],
                 task_log_model: Type[TypeTaskLog],
                 session: Session):
        self.task_model: Type[TypeTask] = task_model
        self.task_log_model: Type[TypeTaskLog] = task_log_model
        self.session: Session = session

    @validated_setter_property
    def task_model(self, value) -> None:
        if not issubclass(value, TaskMixin) or not _is_mapped_class(value):
            raise TypeError("Parameter 'task_model' should be a subclass of "
                            "TaskMixin and a valid mapped class.")

    @validated_setter_property
    def task_log_model(self, value) -> None:
        if not issubclass(value, TaskLogMixin) or not _is_mapped_class(value):
            raise TypeError("Parameter 'task_log_model' should be a subclass "
                            "of TaskLogMixin and a valid mapped class.")

    @validated_setter_property
    def session(self, value) -> None:
        if not isinstance(value, Session):
            raise TypeError("Expected session type object, "
                            "obtained [{}].".format(type(value)))

    def get_task(self,
                 **kwargs: Any) -> TypeTask:
        """Queries the Task by the keyword arguments.

        :param kwargs: keyword arguments for filter_by function in query.
        :return: Query object
        :rtype: :doc:`sqlalchemy.orm.query.Query`
        """
        task = self.session.query(self.task_model)
        task = task.filter_by(**kwargs)
        return task

    def create_task(self,
                    job: TypeJob) -> TypeTask:
        """Creates a task instance with the information in the provided job
        instance.

        :param job: job instance
        :type job: Job
        :return: task instance
        :rtype: Task
        """
        task = self.task_model.create(job)
        task.save(self.session)
        return task

    def create_task_log(self,
                        task_id: int,
                        creator_id: int,
                        comment: str,
                        action: Action) -> TypeTaskLog:
        """Creates a task log.

        :param task_id: id the related task record
        :type task_id: int
        :param creator_id: id of the related user
        :type creator_id: int
        :param comment: comment for the action
        :type comment: str | None
        :param action: action has been taken to the related task
        :type action: TaskAction
        :return: created task log instance
        :rtype: TaskLog
        """
        init_dict = dict(task_id=task_id, creator_id=creator_id,
                         action=action)
        if comment is not None:
            init_dict["comment"] = comment
        task_log = self.task_log_model(**init_dict)
        task_log.save(self.session)
        return task_log

    def duplicate_task(self,
                       task_id: int,
                       commit: bool = True,
                       **kwargs: Any) -> TypeTask:
        """Creates a new task record in DB with the information in the task
        record with the given task_id.

        :param task_id: id the task
        :type task_id: int
        :param commit: whether do the commit to DB, default value is True.
        :type commit: bool
        :param kwargs: keyword arguments to override the information from the
            task record with the given task_id
        :return: created task instance
        :rtype: Task
        """
        return self.task_model.duplicate(self.session, task_id, commit,
                                         **kwargs)

    def start_task(self,
                   job_id: str) -> bool:
        """Checks the status of task record in DB related to the given job_id,
        and decides whether start to execute the task.

        :param job_id: uuid of the job
        :type job_id: str
        :return: start the job if True, else stop the execution.
        :rtype: bool
        """
        # check whether the task instance with given id exists
        task: TypeTask = self.get_task(job_id=job_id)
        task = task.one_or_none()
        if not task:
            return False
        elif task.status in (Status.waiting, Status.postponed,
                             Status.recovered):
            new_status = Status.active
        else:
            # for the other cases, the task can not be started
            return False
        task.status = new_status
        self.session.commit()
        return True

    def finish_task(self,
                    job_id: str,
                    is_succeed: bool,
                    result: str) -> None:
        """Updates the status of task instance in DB with the given id to
        success, if the task function runs successfully, otherwise failed.

        :param job_id: uuid of the job
        :type job_id: str
        :param is_succeed: whether the job is successfully executed.
        :type is_succeed: bool
        :param result: result of the execution of the job with given job_id
        :type result: str
        """
        status = Status.succeed if is_succeed else Status.failed
        task: TypeTask = self.get_task(job_id=job_id)
        task.update(dict(status=status, result=result))
        self.session.commit()
