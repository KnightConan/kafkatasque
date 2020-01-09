import pytest
import unittest.mock as mock
import logging
import kafka
from kafkatasque import abc, db, utils


class TestTasqueABC:
    @pytest.mark.parametrize("logger, tasker, err_msg",
                             [("logger", None,
                               "TasqueABC.logger must be either logging.Logger or "
                               "None. Received <class 'str'>"),
                              (mock.Mock(spec=logging.Logger), "bla",
                               "TasqueABC.tasker must be either Tasker or None. "
                               "Received <class 'str'>")])
    def test___init___fail(self, logger, tasker, err_msg):
        with pytest.raises(TypeError) as err:
            abc.TasqueABC(logger, tasker)
        assert str(err.value) == err_msg

    @pytest.mark.parametrize("logger, tasker",
                             [(None, None),
                              (mock.Mock(spec=logging.Logger), None),
                              (mock.Mock(spec=logging.Logger),
                               mock.Mock(spec=db.Tasker))])
    def test___init__(self, logger, tasker):
        ta = abc.TasqueABC(logger, tasker)
        assert ta.tasker == tasker
        if logger is None:
            assert ta.logger == logging.getLogger('kafkatasque.TasqueABC')
        else:
            assert ta.logger == logger


class TestQueueABC:
    @pytest.mark.parametrize("producer, serializer, logger, tasker, err_msg",
                             [(None, None, None, None,
                               "QueueABC.producer must be KafkaProducer. "
                               "Received <class 'NoneType'>"),
                              (mock.Mock(spec=kafka.KafkaProducer),
                               "serializer", None, None,
                               "QueueABC.serializer must be either callable "
                               "function or None. Received <class 'str'>"),
                              (mock.Mock(spec=kafka.KafkaProducer),
                               None, "logger", None,
                               "QueueABC.logger must be either logging.Logger "
                               "or None. Received <class 'str'>"),
                              (mock.Mock(spec=kafka.KafkaProducer),
                               None, None, "bla",
                               "QueueABC.tasker must be either Tasker or None. "
                               "Received <class 'str'>")])
    def test___init___fail(self, producer, serializer, logger, tasker, err_msg):
        with pytest.raises(TypeError) as err:
            abc.QueueABC(producer, serializer, logger, tasker)
        assert str(err.value) == err_msg

    @pytest.mark.parametrize("producer, serializer, logger, tasker",
                             [(mock.Mock(spec=kafka.KafkaProducer),
                               None, None, None),
                              (mock.Mock(spec=kafka.KafkaProducer),
                               lambda x: x, None, None),
                              (mock.Mock(spec=kafka.KafkaProducer),
                               lambda x: x,
                               mock.Mock(spec=logging.Logger), None),
                              (mock.Mock(spec=kafka.KafkaProducer),
                               lambda x: x,
                               mock.Mock(spec=logging.Logger),
                               mock.Mock(spec=db.Tasker))])
    def test___init__(self, producer, serializer, logger, tasker):
        qa = abc.QueueABC(producer, serializer, logger, tasker)
        assert qa.producer == producer
        assert qa.tasker == tasker
        if serializer is None:
            assert qa.serializer == utils.json_encode
        else:
            assert qa.serializer == serializer
        if logger is None:
            assert qa.logger == logging.getLogger('kafkatasque.QueueABC')
        else:
            assert qa.logger == logger
