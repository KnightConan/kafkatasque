import pytest
import uuid
import unittest.mock as mock
from kafkatasque import job


@pytest.mark.parametrize("topic, value, job_id, timestamp, key, partition, "
                         "func, args, kwargs, timeout, use_db, error_msg",
                         [(1, 1, None, None, None, None, None, None, None, 0,
                           False,
                           "Job.topic must be str. Received <class 'int'>."),
                          ("example", 1, 1, None, None, None, None, None, None,
                           0, False,
                           "Job.id must be either UUID or None. Received "
                           "<class 'int'>."),
                          ("example", 1, None, "111", None, None, None,
                           None, None, 0, False,
                           "Job.timestamp must be either an int or None. "
                           "Received <class 'str'>."),
                          ("example", 1, None, None, 1, None, None,
                           None, None, 0, False,
                           "Job.key must be either string or None. "
                           "Received <class 'int'>."),
                          ("example", 1, None, None, None, "None", None,
                           None, None, 0, False,
                           "Job.partition must be either int or None. "
                           "Received <class 'str'>."),
                          ("example", 1, None, None, None, None, 1,
                           None, None, 0, False,
                           "Job.func must be either None or callable method. "
                           "Received <class 'int'>."),
                          ("example", 1, None, None, None, None, None,
                           1, None, 0, False,
                           "Job.args must be either None or Sequence. "
                           "Received <class 'int'>."),
                          ("example", 1, None, None, None, None, None,
                           None, 1, 0, False,
                           "Job.kwargs must be either None or dictionary. "
                           "Received <class 'int'>."),
                          ("example", 1, None, None, None, None, None,
                           None, None, "0", False,
                           "Job.timeout must be int. Received <class 'str'>."),
                          ("example", 1, None, None, None, None, None,
                           None, None, 0, "None",
                           "Job.use_db must be bool. Received <class 'str'>."),
                          ])
def test_init_error(topic, value, job_id, timestamp, key, partition, func, args,
                    kwargs, timeout, use_db, error_msg):
    with pytest.raises(TypeError) as err:
        job.Job(topic, value, job_id, timestamp, key, partition, func, args,
                kwargs, timeout, use_db)
    assert str(err.value) == error_msg


@pytest.mark.parametrize("param_dict",
                         [dict(topic="example", value=1, job_id=None,
                               timestamp=None, key=None, partition=None,
                               func=None, args=None, kwargs=None, timeout=0,
                               use_db=False),
                          dict(topic="example", value=1, job_id=uuid.uuid4().hex,
                               timestamp=None, key=None, partition=None,
                               func=None, args=None, kwargs=None, timeout=0,
                               use_db=False),
                          dict(topic="example", value=1, job_id=uuid.uuid4().hex,
                               timestamp=111, key=None, partition=None,
                               func=None, args=None, kwargs=None, timeout=0,
                               use_db=False),
                          dict(topic="example", value=1, job_id=uuid.uuid4().hex,
                               timestamp=111, key="bla", partition=None,
                               func=None, args=None, kwargs=None, timeout=0,
                               use_db=False),
                          dict(topic="example", value=1, job_id=uuid.uuid4().hex,
                               timestamp=111, key="bla", partition=10,
                               func=None, args=None, kwargs=None, timeout=0,
                               use_db=False),
                          dict(topic="example", value=1, job_id=uuid.uuid4().hex,
                               timestamp=111, key="bla", partition=10,
                               func=print, args=None, kwargs=None, timeout=0,
                               use_db=False),
                          dict(topic="example", value=1, job_id=uuid.uuid4().hex,
                               timestamp=111, key="bla", partition=10,
                               func=print, args=("a", ), kwargs=None, timeout=0,
                               use_db=False),
                          dict(topic="example", value=1, job_id=uuid.uuid4().hex,
                               timestamp=111, key="bla", partition=10,
                               func=print, args=["a", ], kwargs=None, timeout=0,
                               use_db=False),
                          dict(topic="example", value=1, job_id=uuid.uuid4().hex,
                               timestamp=111, key="bla", partition=10,
                               func=print, args=["a", ], kwargs=dict(b=1),
                               timeout=0, use_db=False),
                          dict(topic="example", value=1, job_id=uuid.uuid4().hex,
                               timestamp=111, key="bla", partition=10,
                               func=print, args=["a", ], kwargs=dict(b=1),
                               timeout=1000, use_db=False),
                          dict(topic="example", value=1, job_id=uuid.uuid4().hex,
                               timestamp=111, key="bla", partition=10,
                               func=print, args=["a", ], kwargs=dict(b=1),
                               timeout=1000, use_db=True),
                          ])
@mock.patch.object(job.Job, "id")
@mock.patch.object(job.time, "time", return_value=11)
@mock.patch.object(job.uuid, "uuid4", return_value=mock.Mock(hex="11"))
def test_init_success(mocked_id, mocked_time, mocked_uuid4, param_dict):
    job_obj = job.Job(**param_dict)
    for key, value in param_dict.items():
        if key == "timestamp":
            if value is None:
                assert 11000 == getattr(job_obj, key)
            else:
                assert value == getattr(job_obj, key)
        elif key == "job_id":
            if value is None:
                assert "11" == getattr(job_obj, "id")
            else:
                assert value == getattr(job_obj, "id")
        else:
            assert value == getattr(job_obj, key)
