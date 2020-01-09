import pytest
import sqlalchemy
import unittest.mock as mock
from kafkatasque import db, constants, job


class TestTasker:

    def test___init___fail_task(self):
        with pytest.raises(TypeError) as err:
            db.Tasker(type("FakeTask", (), {}), None, None)
        assert str(err.value) == "Parameter 'task_model' should be a subclass" \
                                 " of TaskMixin and a valid mapped class."

    def test___init___fail_tasklog(self):
        Base = sqlalchemy.ext.declarative.declarative_base()
        with pytest.raises(TypeError) as err:
            db.Tasker(type("FakeTask1", (db.TaskMixin, Base), {}),
                      type("FakeTaskLog1", (), {}), None)
        assert str(err.value) == "Parameter 'task_log_model' should be a " \
                                 "subclass of TaskLogMixin and a valid mapped" \
                                 " class."

    def test___init___fail_session(self):
        Base = sqlalchemy.ext.declarative.declarative_base()
        with pytest.raises(TypeError) as err:
            db.Tasker(type("FakeTask", (db.TaskMixin, Base), {}),
                      type("FakeTaskLog", (db.TaskLogMixin, Base), {}), None)
        assert str(err.value) == "Expected session type object, obtained " \
                                 "[<class 'NoneType'>]."

    def test___init__(self):
        Base = sqlalchemy.ext.declarative.declarative_base()
        FakeTask = type("FakeTask", (db.TaskMixin, Base), {})
        FakeTaskLog = type("FakeTaskLog", (db.TaskLogMixin, Base), {})
        mocked_session = mock.Mock(spec=sqlalchemy.orm.session.Session)

        tasker = db.Tasker(FakeTask, FakeTaskLog, mocked_session)

        assert tasker.task_model == FakeTask
        assert tasker.task_log_model == FakeTaskLog
        assert tasker.session == mocked_session

    @pytest.fixture
    def tasker(self):
        Base = sqlalchemy.ext.declarative.declarative_base()
        FakeTask = type("FakeTask", (db.TaskMixin, Base), {})
        FakeTaskLog = type("FakeTaskLog", (db.TaskLogMixin, Base), {})
        mocked_session = mock.Mock(spec=sqlalchemy.orm.session.Session)

        return db.Tasker(FakeTask, FakeTaskLog, mocked_session)

    def test_property(self):
        Base = sqlalchemy.ext.declarative.declarative_base()
        FakeTask = type("FakeTask", (db.TaskMixin, Base), {})
        FakeTaskLog = type("FakeTaskLog", (db.TaskLogMixin, Base), {})
        mocked_session = mock.Mock(spec=sqlalchemy.orm.session.Session)

        tasker = db.Tasker(FakeTask, FakeTaskLog, mocked_session)

        assert tasker.task_model == FakeTask
        assert tasker.task_log_model == FakeTaskLog
        assert tasker.session == mocked_session

    @pytest.mark.parametrize("kwargs",
                             [dict(), dict(key=1, value=2)])
    def test_get_task(self, tasker, kwargs):
        conf_dict = {"filter_by.return_value": "task got!"}
        task = mock.Mock(**conf_dict)
        tasker.session.query.return_value = task

        result = tasker.get_task(**kwargs)
        assert result == "task got!"

        tasker.session.query.assert_called_once_with(tasker.task_model)
        task.filter_by.assert_called_once_with(**kwargs)

    @mock.patch.object(db.Tasker, "task_model")
    def test_create_task(self, mocked_task_model, tasker):
        conf_dict = {"save.return_value": "task saved!"}
        task = mock.Mock(**conf_dict)
        task_model_conf = {"create.return_value": task}
        tasker.task_model = mock.Mock(**task_model_conf)
        job_obj = mock.Mock(spec=job.Job, id="111", value="bla", func=None,
                            args=(), kwargs=dict(), timeout=10, use_db=False)

        result = tasker.create_task(job_obj)
        assert result == task

        tasker.task_model.create.assert_called_once_with(job_obj)
        task.save.assert_called_once_with(tasker.session)

    @mock.patch.object(db.Tasker, "task_log_model")
    def test_create_task_log(self, mocked_task_log_model, tasker):
        conf_dict = {"save.return_value": "task log saved!"}
        task_log = mock.Mock(**conf_dict)
        tasker.task_log_model = mock.Mock(return_value=task_log)

        func_kwargs = dict(task_id=1, creator_id=1, comment="comment",
                           action=constants.Action.repeat)
        result = tasker.create_task_log(**func_kwargs)
        assert result == task_log

        tasker.task_log_model.assert_called_once_with(**func_kwargs)
        task_log.save.assert_called_once_with(tasker.session)

    @mock.patch.object(db.Tasker, "task_model")
    def test_duplicate_task(self, mocked_task_model, tasker):
        task_model_conf = {"duplicate.return_value": "duplicated!"}
        tasker.task_model = mock.Mock(**task_model_conf)

        result = tasker.duplicate_task(task_id=1, commit=True, creator_id=22)
        assert result == "duplicated!"

        tasker.task_model.duplicate.assert_called_once_with(tasker.session, 1,
                                                            True,
                                                            creator_id=22)

    def test_start_task_nonexistent(self, tasker):
        conf_dict = {"one_or_none.return_value": None}
        task = mock.Mock(**conf_dict)
        tasker.get_task = mock.Mock(return_value=task)

        result = tasker.start_task("111")
        assert result is False

    @pytest.mark.parametrize("status",
                             [constants.Status.canceled,
                              constants.Status.active,
                              constants.Status.failed,
                              constants.Status.succeed])
    def test_start_task_false(self, tasker, status):
        task = mock.Mock(status=status)
        task.one_or_none.return_value = task
        tasker.get_task = mock.Mock(return_value=task)

        result = tasker.start_task("111")
        assert result is False

    @pytest.mark.parametrize("status",
                             [constants.Status.waiting,
                              constants.Status.postponed,
                              constants.Status.recovered])
    def test_start_task(self, tasker, status):
        task = mock.Mock(status=status)
        task.one_or_none.return_value = task
        tasker.get_task = mock.Mock(return_value=task)

        result = tasker.start_task("111")
        assert result is True
        assert task.status == constants.Status.active
        tasker.session.commit.assert_called_once()

    @pytest.mark.parametrize("job_id, is_succeed, result",
                             [("111", False, "result"),
                              ("111", True, "result")])
    def test_finish_task(self, tasker, job_id, is_succeed, result):
        task = mock.Mock()
        tasker.get_task = mock.Mock(return_value=task)

        tasker.finish_task(job_id, is_succeed, result)
        tasker.get_task.assert_called_once_with(job_id=job_id)
        if is_succeed:
            task.update.assert_called_once_with(
                dict(status=constants.Status.succeed, result=result))
        else:
            task.update.assert_called_once_with(
                dict(status=constants.Status.failed, result=result))
        tasker.session.commit.assert_called_once()


class TestTaskMixin:
    @pytest.fixture
    def Task(self):
        Base = sqlalchemy.ext.declarative.declarative_base()
        type("User", (Base, ), {"__tablename__": "User",
                                "id": sqlalchemy.Column(sqlalchemy.Integer,
                                                        primary_key=True)})
        Task = type("Task", (Base, db.TaskMixin), {})
        type("TaskLog", (Base, db.TaskLogMixin), {})
        return Task

    def test___tablename__(self, Task):
        assert Task.__tablename__ == "task"
