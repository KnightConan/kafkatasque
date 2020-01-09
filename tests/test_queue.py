import pytest
import kafka
import logging
import unittest.mock as mock
from kafkatasque import queue, db, job, utils


class TestDefaultEnqueueAgent:

    @pytest.fixture
    def get_fixtures(self):
        producer = mock.Mock(spec=kafka.KafkaProducer)
        logger = mock.Mock(spec=logging.Logger)
        tasker = mock.Mock(spec=db.Tasker)
        serializer = lambda x: x
        return producer, serializer, logger, tasker

    @pytest.mark.parametrize("producer, serializer, logger, tasker, err_msg",
                             [(None, None, None, None,
                               "DefaultEnqueueAgent.producer must be "
                               "KafkaProducer. Received <class 'NoneType'>"),
                              (mock.Mock(spec=kafka.KafkaProducer), "None",
                               None, None,
                               "DefaultEnqueueAgent.serializer must be either"
                               " callable function or None. Received <class "
                               "'str'>"),
                              (mock.Mock(spec=kafka.KafkaProducer),
                               lambda x: x, "None", None,
                               "DefaultEnqueueAgent.logger must be either "
                               "logging.Logger or None. Received "
                               "<class 'str'>"),
                              (mock.Mock(spec=kafka.KafkaProducer),
                               lambda x: x, mock.Mock(spec=logging.Logger),
                               "bla",
                               "DefaultEnqueueAgent.tasker must be either "
                               "Tasker or None. Received <class 'str'>")])
    def test___init___fail(self, producer, serializer, logger, tasker, err_msg):
        with pytest.raises(TypeError) as err:
            queue.DefaultEnqueueAgent(producer, serializer, logger, tasker)
        assert str(err.value) == err_msg

    @pytest.mark.parametrize("producer, serializer, logger, tasker",
                             [(mock.Mock(spec=kafka.KafkaProducer), None,
                               None, None),
                              (mock.Mock(spec=kafka.KafkaProducer), lambda x: x,
                               None, None),
                              (mock.Mock(spec=kafka.KafkaProducer), lambda x: x,
                               mock.Mock(spec=logging.Logger), None),
                              (mock.Mock(spec=kafka.KafkaProducer), lambda x: x,
                               mock.Mock(spec=logging.Logger),
                               mock.Mock(spec=db.Tasker))])
    def test___init__(self, producer, serializer, logger, tasker):
        dea = queue.DefaultEnqueueAgent(producer, serializer, logger, tasker)
        assert dea.producer == producer
        assert dea.tasker == tasker
        if serializer is None:
            assert dea.serializer == utils.json_encode
        else:
            assert dea.serializer == serializer
        if logger is None:
            assert dea.logger == logging.getLogger(
                'kafkatasque.DefaultEnqueueAgent')
        else:
            assert dea.logger == logger

    def test_create_job(self, get_fixtures):
        with mock.patch.object(job.Job, "__init__", return_value=None) \
                as mocked_init:
            producer, serializer, logger, tasker = get_fixtures
            de = queue.DefaultEnqueueAgent(producer, serializer, logger)
            de.create_job("bla", "bli", None, None, False, None, 0)
            logger.info.assert_called_once_with(
                'Creating Job for topic [bla] with content [bli] ...')
            mocked_init.assert_called_once_with(job_id=None, topic="bla",
                                                key=None, partition=None,
                                                value="bli", func=None, args=(),
                                                kwargs={}, timeout=0,
                                                use_db=False)

    def test_send(self, get_fixtures):
        with mock.patch.object(job.time, 'time', return_value=11) \
                as mocked_time:
            producer, serializer, logger, tasker = get_fixtures
            producer.send.return_value = "message was sent to kafka server"

            de = queue.DefaultEnqueueAgent(producer, serializer, logger)
            job_obj = mock.Mock(spec=job.Job, topic="bla", key="None",
                                partition=1, timestamp=11)
            result = de.send(job_obj)
            logger.info.assert_called_with('Enqueueing {} ...'.format(job_obj))
            producer.send.assert_called_once_with("bla", value=job_obj,
                                                  key="None", partition=1,
                                                  timestamp_ms=11)
            assert result == "message was sent to kafka server"

    def test_enqueue_case_1(self, get_fixtures):
        producer, serializer, logger, tasker = get_fixtures

        job_obj = mock.Mock(spec=job.Job, use_db=False)
        # case 1: content = job instance, use_db = False, tasker is None
        with mock.patch.object(queue.DefaultEnqueueAgent, 'send',
                               return_value="job") as mocked_send:
            de = queue.DefaultEnqueueAgent(producer, serializer, logger)
            result = de.enqueue("bla", job_obj, None, None, False, None, 0)
            mocked_send.assert_called_once_with(job_obj)
            assert result == "job"

    def test_enqueue_case_2(self, get_fixtures):
        producer, serializer, logger, tasker = get_fixtures
        # case 2: content = job instance, use_db = True, tasker is None
        job_obj = mock.Mock(spec=job.Job, use_db=True)
        with pytest.raises(ValueError) as err:
            de = queue.DefaultEnqueueAgent(producer, serializer, logger)
            de.enqueue("bla", job_obj, None, None, True, None, 0)
        assert str(err.value) == "Related Tasker was not provided, " \
                                 "unable to use DB."

    def test_enqueue_case_3(self, get_fixtures):
        producer, serializer, logger, tasker = get_fixtures
        # case 3: content = job instance, use_db = True, tasker is not None
        job_obj = mock.Mock(spec=job.Job, use_db=True)
        with mock.patch.object(queue.DefaultEnqueueAgent, 'send',
                               return_value="job") as mocked_send:
            de = queue.DefaultEnqueueAgent(producer, serializer, logger, tasker)
            result = de.enqueue("bla", job_obj, None, None, True, None, 0)
            logger.info.assert_called_once_with(
                'Saving {} to DB...'.format(job_obj))
            tasker.create_task.assert_called_once_with(job_obj)
            mocked_send.assert_called_once_with(job_obj)
            assert result == "job"

    def test_enqueue_case_4(self, get_fixtures):
        producer, serializer, logger, tasker = get_fixtures
        # case 4: content != job instance, use_db = False, tasker is None
        with mock.patch.object(queue.DefaultEnqueueAgent, 'create_job')\
                as mocked_create_job:
            with mock.patch.object(queue.DefaultEnqueueAgent, 'send',
                                   return_value="job") as mocked_send:
                mocked_create_job.return_value.use_db = False
                de = queue.DefaultEnqueueAgent(producer, serializer, logger)
                result = de.enqueue("bla", "bli", None, None, False, None, 0)
                mocked_create_job.assert_called_once_with("bla", "bli", None,
                                                          None, False, None, 0)
                assert result == "job"

    def test_enqueue_case_5(self, get_fixtures):
        producer, serializer, logger, tasker = get_fixtures

        # case 5: content != job instance, use_db = True, tasker is None
        with mock.patch.object(queue.DefaultEnqueueAgent, 'create_job') \
                as mocked_create_job:
            with pytest.raises(ValueError) as err:
                de = queue.DefaultEnqueueAgent(producer, serializer, logger)
                de.enqueue("bla", "bli", None, None, True, None, 0)
            assert str(err.value) == "Related Tasker was not provided, " \
                                     "unable to use DB."

    def test_enqueue_case_6(self, get_fixtures):
        producer, serializer, logger, tasker = get_fixtures

        # case 6: content != job instance, use_db = True, tasker is not None
        with mock.patch.object(queue.DefaultEnqueueAgent, 'create_job') \
                as mocked_create_job:
            with mock.patch.object(queue.DefaultEnqueueAgent, 'send',
                                   return_value="job") as mocked_send:
                mocked_create_job.return_value.use_db = True
                de = queue.DefaultEnqueueAgent(producer, serializer, logger,
                                               tasker)
                result = de.enqueue("bla", "bli", None, None, True, None, 0)
                mocked_create_job.assert_called_once_with("bla", "bli", None,
                                                          None, True, None, 0)
                logger.info.assert_called_once_with(
                    'Saving {} to DB...'.format(mocked_create_job.return_value))
                tasker.create_task.assert_called_once_with(
                    mocked_create_job.return_value)
                mocked_send.assert_called_once_with(
                    mocked_create_job.return_value)
                assert result == "job"


class TestQueue:

    @pytest.fixture
    def get_fixtures(self):
        producer = mock.Mock(spec=kafka.KafkaProducer,
                             config=dict(bootstrap_servers="localhost"))
        serializer = mock.Mock(spec=utils.json_encode)
        logger = mock.Mock(spec=logging.Logger)
        tasker = mock.Mock(spec=db.Tasker)
        enqueue_agent = mock.Mock(spec=queue.DefaultEnqueueAgent)
        return producer, serializer, logger, tasker, enqueue_agent

    @pytest.fixture
    def queue_obj(self, get_fixtures):
        producer, serializer, logger, tasker, enqueue_agent = get_fixtures
        return queue.Queue(producer, serializer, logger, enqueue_agent, tasker)

    @pytest.mark.parametrize("producer, serializer, logger, enqueue_agent, "
                             "tasker, err_msg",
                             [(None, None, None, None, None,
                               "Queue.producer must be KafkaProducer. "
                               "Received <class 'NoneType'>"),
                              (mock.Mock(spec=kafka.KafkaProducer),
                               "serializer", None, None, None,
                               "Queue.serializer must be either callable "
                               "function or None. Received <class 'str'>"),
                              (mock.Mock(spec=kafka.KafkaProducer),
                               None, "logger", None, None,
                               "Queue.logger must be either logging.Logger or "
                               "None. Received <class 'str'>"),
                              (mock.Mock(spec=kafka.KafkaProducer),
                               None, None, "enqueue_agent", None,
                               "Queue.enqueue_agent must be either EnqueueAgent"
                               " or None. Received <class 'str'>"),
                              (mock.Mock(spec=kafka.KafkaProducer),
                               None, None, None, "bla",
                               "Queue.tasker must be either Tasker or None. "
                               "Received <class 'str'>")])
    @mock.patch.object(queue.Queue, "__del__")
    def test___init___fail(self, mocked_del, producer, serializer, logger,
                           enqueue_agent, tasker, err_msg):
        with pytest.raises(TypeError) as err:
            queue.Queue(producer, serializer, logger, enqueue_agent, tasker)
        assert str(err.value) == err_msg

    def test___init__(self, get_fixtures):
        producer, serializer, logger, tasker, enqueue_agent = get_fixtures

        with mock.patch.object(queue.DefaultEnqueueAgent, "__init__",
                               return_value=None) as mocked_dea_init:
            q = queue.Queue(producer)
            assert q.hosts == "localhost"
            assert q.producer == producer
            assert q.serializer == utils.json_encode
            assert q.logger == logging.getLogger('kafkatasque.Queue')
            assert q.tasker is None
            mocked_dea_init.assert_called_once_with(producer=q.producer,
                                                    serializer=q.serializer,
                                                    logger=q.logger,
                                                    tasker=q.tasker)

        q = queue.Queue(producer, serializer, logger, enqueue_agent, tasker)
        assert q.hosts == "localhost"
        assert q.producer == producer
        assert q.serializer == serializer
        assert q.logger == logger
        assert q.enqueue_agent == enqueue_agent
        assert q.tasker == tasker

    def test___repr__(self, queue_obj):
        assert 'Queue(hosts="localhost")' == repr(queue_obj)

    def test___del__(self, capsys, queue_obj):
        queue_obj.producer.close.side_effect = ValueError("Bla")

        with pytest.raises(ValueError) as e:
            queue_obj.__del__()
        queue_obj.producer.close.assert_called_once()
        captured = capsys.readouterr()
        assert captured.out == 'Failed to close producer due to ({}).\n'.format(
            e.value)

    def test_hosts(self, queue_obj):
        assert "localhost" == queue_obj.hosts

    def test_enqueue(self, get_fixtures, queue_obj):
        producer, serializer, logger, tasker, enqueue_agent = get_fixtures

        queue_obj.enqueue("bla", "bli")
        enqueue_agent.enqueue.assert_called_once_with(topic="bla",
                                                      content="bli",
                                                      key=None, partition=None,
                                                      use_db=False, func=None,
                                                      timeout=0)
