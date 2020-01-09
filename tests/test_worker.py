import pytest
import unittest.mock as mock
import kafka
import logging
from kafkatasque import worker, db, utils


class TestWorker:

    @pytest.fixture
    def get_fixtures(self):
        consumer = mock.Mock(spec=kafka.KafkaConsumer,
                             config=dict(bootstrap_servers="localhost",
                                         group_id="1010"))
        topics = ["topic1", "topic2"]
        deserializer = mock.Mock(side_effect=lambda x: x)
        logger = mock.Mock(spec=logging.Logger)
        tasker = mock.Mock(spec=db.Tasker)
        return consumer, topics, deserializer, logger, tasker

    @pytest.fixture
    def worker_obj(self, get_fixtures):
        consumer, topics, deserializer, logger, tasker = get_fixtures
        consumer.subscription.return_value = ()
        return worker.Worker(consumer, topics, deserializer, logger, tasker)

    @pytest.mark.parametrize("consumer, topics, deserializer, logger, tasker, "
                             "err_msg",
                             [(None, None, None, None, None,
                               "Worker.consumer must be KafkaConsumer. "
                               "Received <class 'NoneType'>"),
                              (mock.Mock(
                                  spec=kafka.KafkaConsumer,
                                  subscription=mock.Mock(return_value=())),
                               {"topic1", "topic2"}, None, None, None,
                               "Worker.topics must be either list or None. "
                               "Received <class 'set'>"),
                              (mock.Mock(
                                  spec=kafka.KafkaConsumer,
                                  subscription=mock.Mock(return_value=())),
                               ["topic1", "topic2"], "deserializer", None, None,
                               "Worker.deserializer must be either callable "
                               "function or None. Received <class 'str'>"),
                              (mock.Mock(
                                  spec=kafka.KafkaConsumer,
                                  subscription=mock.Mock(return_value=())),
                               ["topic1", "topic2"], lambda x: x, "logger",
                               None,
                               "Worker.logger must be either logging.Logger or "
                               "None. Received <class 'str'>"),
                              (mock.Mock(
                                  spec=kafka.KafkaConsumer,
                                  subscription=mock.Mock(return_value=())),
                               ["topic1", "topic2"], lambda x: x,
                               mock.Mock(spec=logging.Logger),
                               "bla",
                               "Worker.tasker must be either Tasker or None. "
                               "Received <class 'str'>")])
    @mock.patch.object(worker.Worker, "__del__")
    def test___init___fail(self, mocked_del, consumer, topics, deserializer,
                           logger, tasker, err_msg):
        with pytest.raises(TypeError) as err:
            worker.Worker(consumer, topics, deserializer, logger, tasker)
        assert str(err.value) == err_msg

    @pytest.mark.parametrize("consumer, topics, deserializer, logger, tasker",
                             [(mock.Mock(spec=kafka.KafkaConsumer), None, None,
                              None, None),
                              (mock.Mock(spec=kafka.KafkaConsumer),
                               ["topic1", "topic2"], None, None, None),
                              (mock.Mock(spec=kafka.KafkaConsumer),
                               ["topic1", "topic2"], lambda x: x, None, None),
                              (mock.Mock(spec=kafka.KafkaConsumer),
                               ["topic1", "topic2"], lambda x: x,
                               mock.Mock(spec=logging.Logger), None),
                              (mock.Mock(spec=kafka.KafkaConsumer),
                               ["topic1", "topic2"], lambda x: x,
                               mock.Mock(spec=logging.Logger),
                               mock.Mock(spec=db.Tasker))])
    def test___init__(self, consumer, topics, deserializer, logger, tasker):
        with mock.patch.object(worker.Worker, "topics_check") \
                as mocked_topics_check:
            worker_obj = worker.Worker(consumer, topics, deserializer, logger,
                                       tasker)
            assert worker_obj.consumer == consumer
            assert worker_obj.topics == topics
            assert worker_obj.tasker == tasker
            mocked_topics_check.assert_called_once()
            if deserializer is None:
                assert worker_obj.deserializer == utils.json_decode
            else:
                assert worker_obj.deserializer == deserializer
            if logger is None:
                assert worker_obj.logger == \
                       logging.getLogger('kafkatasque.Worker')
            else:
                assert worker_obj.logger == logger

    def test_topics_check_error(self, get_fixtures):
        consumer, topics, deserializer, logger, tasker = get_fixtures
        mocked_worker = mock.Mock(spec=worker.Worker, consumer=consumer)
        consumer.subscription.return_value = []
        mocked_worker.topics = []
        with pytest.raises(ValueError) as err:
            worker.Worker.topics_check(mocked_worker)
        assert str(err.value) == "Topics cannot be empty."
        consumer.subscription.assert_called_once()

    def test_topics_check_not_consumer_topics(self, get_fixtures):
        consumer, topics, deserializer, logger, tasker = get_fixtures
        mocked_worker = mock.Mock(spec=worker.Worker, consumer=consumer)

        consumer.subscription.return_value = []
        mocked_worker.topics = topics
        worker.Worker.topics_check(mocked_worker)
        consumer.subscription.assert_called_once()
        consumer.subscribe.assert_called_once_with(topics)

    def test_topics_check_with_topics(self, get_fixtures):
        consumer, topics, deserializer, logger, tasker = get_fixtures
        mocked_worker = mock.Mock(spec=worker.Worker, consumer=consumer)

        consumer.subscription.return_value = ["topic2", "topic3"]
        mocked_worker.topics = topics
        with mock.patch.object(worker.warnings, "warn") as mocked_warn:
            worker.Worker.topics_check(mocked_worker)
            consumer.unsubscribe.assert_called_once()
            consumer.subscribe.assert_called_once_with(topics)
            mocked_warn.assert_called_once_with(
                "Consumer topics are now override with {}".format(topics))

    def test_topics_check_topics_empty(self, get_fixtures):
        consumer, topics, deserializer, logger, tasker = get_fixtures
        mocked_worker = mock.Mock(spec=worker.Worker, consumer=consumer)

        consumer.subscription.return_value = ["topic2", "topic3"]
        mocked_worker.topics = []
        worker.Worker.topics_check(mocked_worker)
        consumer.subscription.assert_called_once()
        consumer.subscribe.assert_not_called()

    def test___repr__(self, worker_obj):
        assert "Worker(hosts=localhost, " \
               "topics=['topic1', 'topic2'], group=1010)" == repr(worker_obj)

    def test___del__(self, capsys, worker_obj):
        worker_obj.consumer.close.side_effect = ValueError("Close Err")
        with pytest.raises(ValueError) as e:
            worker_obj.__del__()
        worker_obj.consumer.close.assert_called_once()
        captured = capsys.readouterr()
        assert captured.out == 'Failed to close consumer due to ' \
                               '{}.\n'.format(e.value)

    @pytest.mark.parametrize("attr_name, expected_result",
                             [("hosts", "localhost"),
                              ('group', "1010")])
    def test_property(self, worker_obj, attr_name, expected_result):
        assert getattr(worker_obj, attr_name) == expected_result

    def test__process_message_invalid_msg(self, worker_obj):
        job = mock.Mock(id="111", value="bla", func=None, args=(),
                        kwargs=dict(), timeout=10, use_db=False)
        msg = mock.Mock(topic="topic 1", partition=1, offset=0, value=job)

        worker_obj.deserializer = None
        worker_obj._process_message(msg)
        worker_obj.logger.info.assert_called_once_with(
            'Processing Message(topic=topic 1, partition=1, offset=0) ...')
        worker_obj.logger.exception.assert_called_once_with(
            "Invalid message content: 'NoneType' object is not callable")

    def test__process_message_use_db_error(self, worker_obj):
        job = mock.Mock(id="111", value="bla", func=None, args=(),
                        kwargs=dict(), timeout=10, use_db=False)
        msg = mock.Mock(topic="topic 1", partition=1, offset=0, value=job)
        msg.value.use_db = True
        worker_obj.tasker = None
        with pytest.raises(ValueError) as err:
            worker_obj._process_message(msg)
        worker_obj.deserializer.assert_called_once_with(job)
        worker_obj.logger.info.assert_called_once_with(
            'Processing Message(topic=topic 1, partition=1, offset=0) ...')
        assert str(err.value) == "Related tasker was not provided, unable to " \
                                 "use DB."

    def test__process_message(self, worker_obj):
        job = mock.Mock(id="111", value="bla", func=None, args=(),
                        kwargs=dict(), timeout=10, use_db=True)
        msg = mock.Mock(topic="topic 1", partition=1, offset=0, value=job)
        worker_obj._execute_job = mock.Mock()

        worker_obj._process_message(msg)
        worker_obj.deserializer.assert_called_once_with(job)
        logger_info_calls = [
            mock.call('Processing Message(topic=topic 1, partition=1, '
                      'offset=0) ...'),
            mock.call("Executing job 111: None('bla')")
        ]
        assert worker_obj.logger.info.call_count == 2
        worker_obj.logger.info.assert_has_calls(logger_info_calls)
        worker_obj._execute_job.assert_called_once_with(job)

    def test_start_error(self, worker_obj):
        worker_obj.consumer.config["group_id"] = None

        with pytest.raises(ValueError) as err:
            worker_obj.start(commit_offsets=True)
        assert str(err.value) == "If group id is None, commit_offsets must be" \
                                 " False."

    @pytest.mark.parametrize("commit_offsets", [False, True])
    def test_start_no_error(self, worker_obj, commit_offsets):
        worker_obj.consumer.__iter__ = mock.Mock(
            return_value=iter(['msg 1', 'msg 2', 'msg 3']))
        worker_obj._process_message = mock.Mock()
        calls = [mock.call('msg 1'), mock.call('msg 2'), mock.call('msg 3')]

        worker_obj.start(commit_offsets)
        worker_obj.logger.info.called_once_with(
            'Starting {} ...'.format(worker_obj))
        assert worker_obj._process_message.call_count == 3
        worker_obj._process_message.assert_has_calls(calls)
        if commit_offsets:
            assert worker_obj.consumer.commit.call_count == 3

    # has timeout, use db and has tasker and not execute task
    def test__execute_job_case_1(self, worker_obj):
        job = mock.Mock(id="111", value="bla", func=None, args=(),
                        kwargs=dict(), timeout=10, use_db=True)
        worker_obj.tasker.start_task = mock.Mock(return_value=False)

        with mock.patch.object(worker.threading.Timer, "start") as mocked_start:
            with mock.patch.object(worker.threading.Timer,
                                   "cancel") as mocked_cancel:
                worker_obj._execute_job(job)
                worker_obj.logger.info.assert_called_once_with(
                    'Job 111 was not executed.')
                mocked_start.assert_called_once()
                mocked_cancel.assert_not_called()
                worker_obj.tasker.start_task.assert_called_once_with("111")
                worker_obj.tasker.finish_task.assert_not_called()

    # no timeout, use db and has tasker and not execute task
    def test__execute_job_case_2(self, worker_obj):
        job = mock.Mock(id="111", value="bla", func=None, args=(),
                        kwargs=dict(), timeout=0, use_db=True)
        worker_obj.tasker.start_task = mock.Mock(return_value=False)

        with mock.patch.object(worker.threading.Timer, "start") as mocked_start:
            with mock.patch.object(worker.threading.Timer,
                                   "cancel") as mocked_cancel:
                worker_obj._execute_job(job)
                worker_obj.logger.info.assert_called_once_with(
                    'Job 111 was not executed.')
                mocked_start.assert_not_called()
                mocked_cancel.assert_not_called()
                worker_obj.tasker.start_task.assert_called_once_with("111")
                worker_obj.tasker.finish_task.assert_not_called()

    # has timeout, use db and execute task, func is not callable,
    def test__execute_job_case_3(self, worker_obj):
        job = mock.Mock(id="111", value="bla", func=None, args=(),
                        kwargs=dict(), timeout=10, use_db=True)
        worker_obj.tasker.start_task = mock.Mock(return_value=True)

        with mock.patch.object(worker.threading.Timer, "start") as mocked_start:
            with mock.patch.object(worker.threading.Timer,
                                   "cancel") as mocked_cancel:
                worker_obj._execute_job(job)
                worker_obj.logger.info.assert_called_once_with(
                    "Job 111 returned: None")
                mocked_start.assert_called_once()
                mocked_cancel.assert_called_once()
                worker_obj.tasker.start_task.assert_called_once_with("111")
                worker_obj.tasker.finish_task.assert_called_once_with(
                    "111", True, "None")

    # no timeout, use db and has tasker and execute task, func is not callable,
    def test__execute_job_case_4(self, worker_obj):
        job = mock.Mock(id="111", value="bla", func=None, args=(),
                        kwargs=dict(), timeout=0, use_db=True)
        worker_obj.tasker.start_task = mock.Mock(return_value=True)

        with mock.patch.object(worker.threading.Timer, "start") as mocked_start:
            with mock.patch.object(worker.threading.Timer,
                                   "cancel") as mocked_cancel:
                worker_obj._execute_job(job)
                worker_obj.logger.info.assert_called_once_with(
                    "Job 111 returned: None")
                mocked_start.assert_not_called()
                mocked_cancel.assert_not_called()
                worker_obj.tasker.start_task.assert_called_once_with("111")
                worker_obj.tasker.finish_task.assert_called_once_with(
                    "111", True, "None")

    # no timeout, not use db and execute task, func is not callable,
    def test__execute_job_case_5(self, worker_obj):
        job = mock.Mock(id="111", value="bla", func=None, args=(),
                        kwargs=dict(), timeout=0, use_db=False)
        worker_obj.tasker.start_task = mock.Mock(return_value=True)

        with mock.patch.object(worker.threading.Timer, "start") as mocked_start:
            with mock.patch.object(worker.threading.Timer,
                                   "cancel") as mocked_cancel:
                worker_obj._execute_job(job)
                worker_obj.logger.info.assert_called_once_with(
                    "Job 111 returned: None")
                mocked_start.assert_not_called()
                mocked_cancel.assert_not_called()
                worker_obj.tasker.start_task.assert_not_called()
                worker_obj.tasker.finish_task.assert_not_called()

    # has timeout, not use db and execute task, func is not callable,
    def test__execute_job_case_6(self, worker_obj):
        job = mock.Mock(id="111", value="bla", func=None, args=(),
                        kwargs=dict(), timeout=10, use_db=False)
        worker_obj.tasker.start_task = mock.Mock(return_value=True)

        with mock.patch.object(worker.threading.Timer, "start") as mocked_start:
            with mock.patch.object(worker.threading.Timer,
                                   "cancel") as mocked_cancel:
                worker_obj._execute_job(job)
                worker_obj.logger.info.assert_called_once_with(
                    "Job 111 returned: None")
                mocked_start.assert_called_once()
                mocked_cancel.assert_called_once()
                worker_obj.tasker.start_task.assert_not_called()
                worker_obj.tasker.finish_task.assert_not_called()

    # has timeout, use db and execute task, func is callable and no error,
    def test__execute_job_case_7(self, worker_obj):
        job = mock.Mock(id="111", value="bla",
                        func=mock.Mock(return_value="dummy"),
                        args=("bli", "blu"),
                        kwargs=dict(input3="ble"), timeout=10, use_db=True)
        worker_obj.tasker.start_task = mock.Mock(return_value=True)

        with mock.patch.object(worker.threading.Timer, "start") as mocked_start:
            with mock.patch.object(worker.threading.Timer,
                                   "cancel") as mocked_cancel:
                worker_obj._execute_job(job)
                worker_obj.logger.info.assert_called_once_with(
                    "Job 111 returned: dummy")
                mocked_start.assert_called_once()
                mocked_cancel.assert_called_once()
                worker_obj.tasker.start_task.assert_called_once_with("111")
                worker_obj.tasker.finish_task.assert_called_once_with(
                    "111", True, "dummy")
                job.func.assert_called_once_with("bla", "bli", "blu",
                                                 input3="ble")

    # no timeout, use db and execute task, func is callable and no error,
    def test__execute_job_case_8(self, worker_obj):
        job = mock.Mock(id="111", value="bla",
                        func=mock.Mock(return_value="dummy"),
                        args=("bli", "blu"),
                        kwargs=dict(input3="ble"), timeout=0, use_db=True)
        worker_obj.tasker.start_task = mock.Mock(return_value=True)

        with mock.patch.object(worker.threading.Timer, "start") as mocked_start:
            with mock.patch.object(worker.threading.Timer,
                                   "cancel") as mocked_cancel:
                worker_obj._execute_job(job)
                worker_obj.logger.info.assert_called_once_with(
                    "Job 111 returned: dummy")
                mocked_start.assert_not_called()
                mocked_cancel.assert_not_called()
                worker_obj.tasker.start_task.assert_called_once_with("111")
                worker_obj.tasker.finish_task.assert_called_once_with(
                    "111", True, "dummy")
                job.func.assert_called_once_with("bla", "bli", "blu",
                                                 input3="ble")

    # no timeout, not use db and execute task, func is callable and no error,
    def test__execute_job_case_9(self, worker_obj):
        job = mock.Mock(id="111", value="bla",
                        func=mock.Mock(return_value="dummy"),
                        args=("bli", "blu"),
                        kwargs=dict(input3="ble"), timeout=0, use_db=False)
        worker_obj.tasker.start_task = mock.Mock(return_value=True)

        with mock.patch.object(worker.threading.Timer, "start") as mocked_start:
            with mock.patch.object(worker.threading.Timer,
                                   "cancel") as mocked_cancel:
                worker_obj._execute_job(job)
                worker_obj.logger.info.assert_called_once_with(
                    "Job 111 returned: dummy")
                mocked_start.assert_not_called()
                mocked_cancel.assert_not_called()
                worker_obj.tasker.start_task.assert_not_called()
                worker_obj.tasker.finish_task.assert_not_called()
                job.func.assert_called_once_with("bla", "bli", "blu",
                                                 input3="ble")

    # has timeout, not use db and execute task, func is callable and no error,
    def test__execute_job_case_10(self, worker_obj):
        job = mock.Mock(id="111", value="bla",
                        func=mock.Mock(return_value="dummy"),
                        args=("bli", "blu"),
                        kwargs=dict(input3="ble"), timeout=10, use_db=False)
        worker_obj.tasker.start_task = mock.Mock(return_value=True)

        with mock.patch.object(worker.threading.Timer, "start") as mocked_start:
            with mock.patch.object(worker.threading.Timer,
                                   "cancel") as mocked_cancel:
                worker_obj._execute_job(job)
                worker_obj.logger.info.assert_called_once_with(
                    "Job 111 returned: dummy")
                mocked_start.assert_called_once()
                mocked_cancel.assert_called_once()
                worker_obj.tasker.start_task.assert_not_called()
                worker_obj.tasker.finish_task.assert_not_called()
                job.func.assert_called_once_with("bla", "bli", "blu",
                                                 input3="ble")

    # has timeout, use db and execute task, func is callable and
    # KeyboardInterrupt
    def test__execute_job_case_11(self, worker_obj):
        job = mock.Mock(id="111", value="bla",
                        func=mock.Mock(side_effect=KeyboardInterrupt),
                        args=("bli", "blu"), kwargs=dict(input3="ble"),
                        timeout=10, use_db=True)
        worker_obj.tasker.start_task = mock.Mock(return_value=True)

        with mock.patch.object(worker.threading.Timer, "start") as mocked_start:
            with mock.patch.object(worker.threading.Timer,
                                   "cancel") as mocked_cancel:
                worker_obj._execute_job(job)
                worker_obj.logger.error.assert_called_once_with(
                    "Job 111 timed out or was interrupted")
                mocked_start.assert_called_once()
                mocked_cancel.assert_called_once()
                worker_obj.tasker.start_task.assert_called_once_with("111")
                worker_obj.tasker.finish_task.assert_called_once_with(
                    "111", False, "Job 111 timed out or was interrupted")
                job.func.assert_called_once_with("bla", "bli", "blu",
                                                 input3="ble")

    # no timeout, use db and execute task, func is callable and
    # KeyboardInterrupt
    def test__execute_job_case_12(self, worker_obj):
        job = mock.Mock(id="111", value="bla",
                        func=mock.Mock(side_effect=KeyboardInterrupt),
                        args=("bli", "blu"), kwargs=dict(input3="ble"),
                        timeout=0, use_db=True)
        worker_obj.tasker.start_task = mock.Mock(return_value=True)

        with mock.patch.object(worker.threading.Timer, "start") as mocked_start:
            with mock.patch.object(worker.threading.Timer,
                                   "cancel") as mocked_cancel:
                worker_obj._execute_job(job)
                worker_obj.logger.error.assert_called_once_with(
                    "Job 111 timed out or was interrupted")
                mocked_start.assert_not_called()
                mocked_cancel.assert_not_called()
                worker_obj.tasker.start_task.assert_called_once_with("111")
                worker_obj.tasker.finish_task.assert_called_once_with(
                    "111", False, "Job 111 timed out or was interrupted")
                job.func.assert_called_once_with("bla", "bli", "blu",
                                                 input3="ble")

    # no timeout, not use db and execute task, func is callable and
    # KeyboardInterrupt
    def test__execute_job_case_13(self, worker_obj):
        job = mock.Mock(id="111", value="bla",
                        func=mock.Mock(side_effect=KeyboardInterrupt),
                        args=("bli", "blu"), kwargs=dict(input3="ble"),
                        timeout=0, use_db=False)
        worker_obj.tasker.start_task = mock.Mock(return_value=True)

        with mock.patch.object(worker.threading.Timer, "start") as mocked_start:
            with mock.patch.object(worker.threading.Timer,
                                   "cancel") as mocked_cancel:
                worker_obj._execute_job(job)
                worker_obj.logger.error.assert_called_once_with(
                    "Job 111 timed out or was interrupted")
                mocked_start.assert_not_called()
                mocked_cancel.assert_not_called()
                worker_obj.tasker.start_task.assert_not_called()
                worker_obj.tasker.finish_task.assert_not_called()
                job.func.assert_called_once_with("bla", "bli", "blu",
                                                 input3="ble")

    # has timeout, not use db and execute task, func is callable and
    # KeyboardInterrupt
    def test__execute_job_case_14(self, worker_obj):
        job = mock.Mock(id="111", value="bla",
                        func=mock.Mock(side_effect=KeyboardInterrupt),
                        args=("bli", "blu"), kwargs=dict(input3="ble"),
                        timeout=10, use_db=False)
        worker_obj.tasker.start_task = mock.Mock(return_value=True)

        with mock.patch.object(worker.threading.Timer, "start") as mocked_start:
            with mock.patch.object(worker.threading.Timer,
                                   "cancel") as mocked_cancel:
                worker_obj._execute_job(job)
                worker_obj.logger.error.assert_called_once_with(
                    "Job 111 timed out or was interrupted")
                mocked_start.assert_called_once()
                mocked_cancel.assert_called_once()
                worker_obj.tasker.start_task.assert_not_called()
                worker_obj.tasker.finish_task.assert_not_called()
                job.func.assert_called_once_with("bla", "bli", "blu",
                                                 input3="ble")

    # has timeout, use db and execute task, func is callable and other exception
    def test__execute_job_case_15(self, worker_obj):
        job = mock.Mock(id="111", value="bla",
                        func=mock.Mock(
                            side_effect=AttributeError("wrong attr")),
                        args=("bli", "blu"), kwargs=dict(input3="ble"),
                        timeout=10, use_db=True)
        worker_obj.tasker.start_task = mock.Mock(return_value=True)

        with mock.patch.object(worker.threading.Timer, "start") as mocked_start:
            with mock.patch.object(worker.threading.Timer,
                                   "cancel") as mocked_cancel:
                worker_obj._execute_job(job)
                worker_obj.logger.exception.assert_called_once_with(
                    "Job 111 raised an exception: wrong attr")
                mocked_start.assert_called_once()
                mocked_cancel.assert_called_once()
                worker_obj.tasker.start_task.assert_called_once_with("111")
                worker_obj.tasker.finish_task.assert_called_once_with(
                    "111", False, "wrong attr")
                job.func.assert_called_once_with("bla", "bli", "blu",
                                                 input3="ble")

    # no timeout, use db and execute task, func is callable and other exception
    def test__execute_job_case_16(self, worker_obj):
        job = mock.Mock(id="111", value="bla",
                        func=mock.Mock(
                            side_effect=AttributeError("wrong attr")),
                        args=("bli", "blu"), kwargs=dict(input3="ble"),
                        timeout=0, use_db=True)
        worker_obj.tasker.start_task = mock.Mock(return_value=True)

        with mock.patch.object(worker.threading.Timer, "start") as mocked_start:
            with mock.patch.object(worker.threading.Timer,
                                   "cancel") as mocked_cancel:
                worker_obj._execute_job(job)
                worker_obj.logger.exception.assert_called_once_with(
                    "Job 111 raised an exception: wrong attr")
                mocked_start.assert_not_called()
                mocked_cancel.assert_not_called()
                worker_obj.tasker.start_task.assert_called_once_with("111")
                worker_obj.tasker.finish_task.assert_called_once_with(
                    "111", False, "wrong attr")
                job.func.assert_called_once_with("bla", "bli", "blu",
                                                 input3="ble")

    # no timeout, not use db and execute task, func is callable and other
    # exception
    def test__execute_job_case_17(self, worker_obj):
        job = mock.Mock(id="111", value="bla",
                        func=mock.Mock(
                            side_effect=AttributeError("wrong attr")),
                        args=("bli", "blu"), kwargs=dict(input3="ble"),
                        timeout=0, use_db=False)
        worker_obj.tasker.start_task = mock.Mock(return_value=True)

        with mock.patch.object(worker.threading.Timer, "start") as mocked_start:
            with mock.patch.object(worker.threading.Timer,
                                   "cancel") as mocked_cancel:
                worker_obj._execute_job(job)
                worker_obj.logger.exception.assert_called_once_with(
                    "Job 111 raised an exception: wrong attr")
                mocked_start.assert_not_called()
                mocked_cancel.assert_not_called()
                worker_obj.tasker.start_task.assert_not_called()
                worker_obj.tasker.finish_task.assert_not_called()
                job.func.assert_called_once_with("bla", "bli", "blu",
                                                 input3="ble")

    # has timeout, not use db and execute task, func is callable and other
    # exception
    def test__execute_job_case_18(self, worker_obj):
        job = mock.Mock(id="111", value="bla",
                        func=mock.Mock(
                            side_effect=AttributeError("wrong attr")),
                        args=("bli", "blu"), kwargs=dict(input3="ble"),
                        timeout=10, use_db=False)
        worker_obj.tasker.start_task = mock.Mock(return_value=True)

        with mock.patch.object(worker.threading.Timer, "start") as mocked_start:
            with mock.patch.object(worker.threading.Timer,
                                   "cancel") as mocked_cancel:
                worker_obj._execute_job(job)
                worker_obj.logger.exception.assert_called_once_with(
                    "Job 111 raised an exception: wrong attr")
                mocked_start.assert_called_once()
                mocked_cancel.assert_called_once()
                worker_obj.tasker.start_task.assert_not_called()
                worker_obj.tasker.finish_task.assert_not_called()
                job.func.assert_called_once_with("bla", "bli", "blu",
                                                 input3="ble")
