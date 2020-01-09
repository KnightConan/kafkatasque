import pytest
import unittest.mock as mock
from kafkatasque import schedule, queue, db, constants, job


class TestTaduler:

    @pytest.fixture
    def get_fixtures(self):
        task = mock.Mock(job_id=1, job_topic="topic", job_value="bla",
                         job_key=None, job_partition=1, job_func=None,
                         job_args=(), job_kwargs={}, job_timeout=0)
        tasker = mock.Mock(spec=db.Tasker,
                           get_task=mock.Mock(return_value=task))
        q = mock.Mock(spec=queue.Queue, tasker=tasker)
        return task, tasker, q

    @pytest.fixture
    def taduler(self, get_fixtures):
        task, tasker, q = get_fixtures
        return schedule.Taduler(q)

    def test___init__(self, get_fixtures):
        with pytest.raises(TypeError) as err:
            schedule.Taduler(None)
        assert str(err.value) == "Taduler.queue must be Queue. Received " \
                                 "<class 'NoneType'>"

        with pytest.raises(TypeError) as err:
            que = mock.Mock(spec=queue.Queue)
            schedule.Taduler(que)
        assert str(err.value) == "Taduler.queue must be Queue with not None " \
                                 "value tasker. Received <class " \
                                 "'unittest.mock.Mock'>"

        task, tasker, q = get_fixtures
        taduler = schedule.Taduler(q)
        assert taduler.queue == q

    def test__resend_task_nonexistent_task(self, get_fixtures, taduler):
        task, tasker, q = get_fixtures

        task.one_or_none.return_value = None
        result = taduler._resend_task(1)
        assert result is None

    @pytest.mark.parametrize("task_status, task_id, called_params",
                             [(constants.Status.recovered, 1,
                               dict(job_id=1, topic="topic", content="bla",
                                    key=None, partition=1, func=None, timeout=0,
                                    use_db=True)),
                              (constants.Status.postponed, 11,
                               dict(job_id=1, topic="topic", content="bla",
                                    key=None, partition=1, func=None, timeout=0,
                                    use_db=True)),
                              (constants.Status.waiting, 111,
                               dict(job_id=None, topic="topic", content="bla",
                                    key=None, partition=1, func=None, timeout=0,
                                    use_db=True)),
                              ])
    def test__resend_task(self, get_fixtures, taduler, task_status, task_id,
                          called_params):
        task, tasker, q = get_fixtures
        task.one_or_none.return_value = task
        job_obj = mock.Mock(spec=job.Job, id=1000)
        q.enqueue_agent.create_job.return_value = job_obj
        q.enqueue_agent.send.return_value = "sent!"

        task.status = task_status
        result = taduler._resend_task(task_id)
        q.enqueue_agent.create_job.assert_called_once_with(**called_params)
        q.enqueue_agent.send.assert_called_once_with(job_obj)
        assert result == "sent!"
        if task_status not in (constants.Status.recovered,
                               constants.Status.postponed):
            tasker.session.commit.assert_called_once()
            assert task.job_id == 1000

    @pytest.mark.parametrize("func_params, expected_result",
                             [(dict(task_id=1, creator_id=1, comment=None,
                                    action=constants.Action.cancel,
                                    new_status=constants.Status.canceled),
                               None),
                              (dict(task_id=1, creator_id=1, comment=None,
                                    action=constants.Action.postpone,
                                    new_status=constants.Status.postponed),
                               "sent!"),
                              ])
    def test__schedule_task_with_new_status(self, get_fixtures, taduler,
                                            func_params, expected_result):
        task, tasker, q = get_fixtures
        taduler._resend_task = mock.Mock(return_value=expected_result)
        called_params = dict(task_id=func_params["task_id"],
                             creator_id=func_params["creator_id"],
                             comment=func_params["comment"],
                             action=func_params["action"])

        # test case 1: return None
        result = taduler._schedule_task(**func_params)
        tasker.create_task_log.assert_called_once_with(**called_params)
        tasker.get_task.assert_called_once_with(id=func_params["task_id"])
        task.update.assert_called_once_with(
            dict(status=func_params["new_status"]))
        tasker.session.commit.assert_called_once()
        assert result == expected_result
        if expected_result is not None:
            taduler._resend_task.assert_called_once_with(
                task_id=func_params["task_id"])

    @pytest.mark.parametrize("func_params, called_task_id, expected_result",
                             [(dict(task_id=1, creator_id=1, comment="12",
                                    action=constants.Action.recover),
                               12, "sent!"),
                              (dict(task_id=1, creator_id=1, comment="aa",
                                    action=constants.Action.recover,
                                    new_status=None),
                               1, "sent!"),
                              ])
    def test__schedule_task_without_new_status(self, get_fixtures, taduler,
                                               func_params, called_task_id,
                                               expected_result):
        task, tasker, q = get_fixtures
        taduler._resend_task = mock.Mock(return_value=expected_result)
        called_params = dict(task_id=func_params["task_id"],
                             creator_id=func_params["creator_id"],
                             comment=func_params["comment"],
                             action=func_params["action"])

        # test case 3: return sent
        result = taduler._schedule_task(**func_params)
        tasker.create_task_log.assert_called_once_with(**called_params)
        taduler._resend_task.assert_called_once_with(task_id=called_task_id)
        assert result == expected_result

    @pytest.mark.parametrize("func_name, func_params, called_params",
                             [("postpone_task",
                               dict(task_id=1, creator_id=10),
                               dict(task_id=1,
                                    creator_id=10,
                                    comment=None,
                                    action=constants.Action.postpone,
                                    new_status=constants.Status.postponed)),
                              ("postpone_task",
                               dict(task_id=1, creator_id=10,
                                    comment="postpone"),
                               dict(task_id=1,
                                    creator_id=10,
                                    comment="postpone",
                                    action=constants.Action.postpone,
                                    new_status=constants.Status.postponed)),
                              ("cancel_task",
                               dict(task_id=1, creator_id=10),
                               dict(task_id=1,
                                    creator_id=10,
                                    comment=None,
                                    action=constants.Action.cancel,
                                    new_status=constants.Status.canceled)),
                              ("cancel_task",
                               dict(task_id=1, creator_id=10,
                                    comment="cancel"),
                               dict(task_id=1,
                                    creator_id=10,
                                    comment="cancel",
                                    action=constants.Action.cancel,
                                    new_status=constants.Status.canceled)),
                              ("recover_task",
                               dict(task_id=1, creator_id=10, comment=None),
                               dict(task_id=1,
                                    creator_id=10,
                                    comment=None,
                                    action=constants.Action.recover)),
                              ("recover_task",
                               dict(task_id=1, creator_id=10,
                                    comment="recover"),
                               dict(task_id=1,
                                    creator_id=10,
                                    comment="recover",
                                    action=constants.Action.recover)),
                              ("repeat_task",
                               dict(task_id=1, creator_id=10),
                               dict(task_id=1,
                                    creator_id=10,
                                    comment="11",
                                    action=constants.Action.repeat)),
                              ])
    def test_schedule_task(self, get_fixtures, taduler, func_name, func_params,
                           called_params):
        task, tasker, q = get_fixtures
        tasker.duplicate_task.return_value = mock.Mock(id=11)
        taduler._schedule_task = mock.Mock(return_value="rescheduled")

        result = getattr(taduler, func_name)(**func_params)
        taduler._schedule_task.assert_called_once_with(**called_params)
        assert result == "rescheduled"
        if func_name == "repeat_task":
            tasker.duplicate_task.assert_called_once_with(1)
