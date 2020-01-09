import pytest
import unittest.mock as mock
from sqlalchemy.orm.session import Session
from kafkatasque import utils
from uuid import uuid4


@pytest.mark.parametrize("value, validation, expected_result",
                         [(None, False, True),
                          (None, True, True),
                          (1, 1 > 2, False),
                          (1, 1 < 2, True)])
def test_is_optional(value, validation, expected_result):
    result = utils.is_optional(value, validation)
    assert result == expected_result


@pytest.mark.parametrize("value, datatype, extra_check, expected_result",
                         [(1, int, None, True),
                          (1, int, 1 > 2, False),
                          (1, int, 1 < 2, True),
                          (1, str, None, False),
                          (1, str, 1 > 2, False),
                          (1, str, 1 < 2, False),
                          (None, int, None, True),
                          (None, int, False, True),
                          (None, int, True, True)])
def test_is_optional_type(value, datatype, extra_check, expected_result):
    result = utils.is_optional_type(value, datatype, extra_check)
    assert result == expected_result


class Thing(object):
    def __init__(self, name):
        self.name = name


@pytest.mark.parametrize("obj, to_bytes, expected_result",
                         [(Thing("Awesome"), False,
                           '{"py/object": "tests.test_utils.Thing", "name": "Awesome"}'),
                          (Thing("Awesome"), True,
                           b'{"py/object": "tests.test_utils.Thing", "name": "Awesome"}'),
                          (None, False, 'null'),
                          (None, True, b'null'),
                          ("awesome", False, '"awesome"'),
                          ("awesome", True, b'"awesome"'),
                          (1, False, '1'),
                          (1, True, b'1'),
                          ({"a": 1, "b": 'b"b"'}, False, '{"a": 1, "b": "b\\"b\\""}'),
                          ({"a": 1, "b": 'b"b"'}, True, b'{"a": 1, "b": "b\\"b\\""}'),
                          ([1, "2"], False, '[1, "2"]'),
                          ([1, "2"], True, b'[1, "2"]')])
def test_json_encode(obj, to_bytes, expected_result):
    result = utils.json_encode(obj, to_bytes)
    if to_bytes:
        assert isinstance(result, bytes)
    else:
        assert isinstance(result, str)
    assert result == expected_result


@pytest.mark.parametrize("obj, expected_type, expected_result",
                         [('{"name": "Awesome", "py/object": "tests.test_utils.Thing"}',
                           Thing, Thing("Awesome")),
                          (b'{"name": "Awesome", "py/object": "tests.test_utils.Thing"}',
                           Thing, Thing("Awesome")),
                          (None, None, None)])
def test_json_decode(obj, expected_type, expected_result):
    if isinstance(obj, (str, bytes)):
        result = utils.json_decode(obj)
        assert isinstance(result, expected_type)
        assert result.name == expected_result.name
    else:
        with pytest.raises(ValueError) as err:
            utils.json_decode(obj)
        assert str(err.value) == "Invalid input, expected bytes or string. " \
                                 "Received [<class 'NoneType'>]"


@pytest.mark.parametrize("uuid_hex_str, expected_result",
                         [(1111, False),
                          ("1111", False),
                          (uuid4().hex, True)])
def test_validate_uuid4(uuid_hex_str, expected_result):
    # test case missing, valid hex code but invalid uuid4
    result = utils.validate_uuid4(uuid_hex_str)
    assert result == expected_result


@pytest.mark.parametrize("value, default_value, expected_result",
                         [(111, 10, 111),
                          ("a", 10, 10),
                          ("111", 10, 111),
                          ("111.9", 10, 111),
                          (111.9, 10, 111)])
def test_ensure_int(value, default_value, expected_result):
    result = utils.ensure_int(value, default_value)
    assert result == expected_result


class Example:
    def __init__(self):
        self.a = 1

    def get(self):
        return self.a

    def tick(self):
        print("something")
    __call__ = tick


@pytest.mark.parametrize("func, args, kwargs, expected_result",
                         [(Example().get, [], {},
                           "tests.test_utils.Example.get([], {})"),
                          (lambda x: x, ["1"], {},
                           "tests.test_utils.<lambda>(['1'], {})"),
                          (print, ["1"], {}, "builtins.print(['1'], {})"),
                          (Example(), [], {},
                           "tests.test_utils.Example([], {})"),
                          (Example, [], {},
                           "<class 'tests.test_utils.Example'>([], {})")])
def test_repr_func(func, args, kwargs, expected_result):
    result = utils.repr_func(func, args, kwargs)
    assert result == expected_result


@utils.session_required
def dummy(session, a):
    return a


class Dumm:
    @utils.session_required
    def dumm1(self, session, a):
        return a

    @classmethod
    @utils.session_required
    def dumm2(cls, session, a):
        return a


def test_session_required_function():
    with pytest.raises(TypeError) as e:
        dummy(11, "11")
    assert str(e.value) == "Parameter 'session' should be SQLAlchemy Session." \
                           " Received {}.".format(int)

    with pytest.raises(TypeError) as e:
        dummy(a=11, session="11")
    assert str(e.value) == "Parameter 'session' should be SQLAlchemy Session." \
                           " Received {}.".format(str)

    res = dummy(a=11, session=mock.Mock(spec=Session))
    assert res == 11

    res = dummy(mock.Mock(spec=Session), "11")
    assert res == "11"


def test_session_required_instance_function():
    with pytest.raises(TypeError) as e:
        Dumm().dumm1(11, "11")
    assert str(e.value) == "Parameter 'session' should be SQLAlchemy Session." \
                           " Received {}.".format(int)

    with pytest.raises(TypeError) as e:
        Dumm().dumm1(a=11, session="11")
    assert str(e.value) == "Parameter 'session' should be SQLAlchemy Session." \
                           " Received {}.".format(str)

    res = Dumm().dumm1(a=11, session=mock.Mock(spec=Session))
    assert res == 11

    res = Dumm().dumm1(mock.Mock(spec=Session), "11")
    assert res == "11"


def test_session_required_classmethod():
    with pytest.raises(TypeError) as e:
        Dumm.dumm2(11, "11")
    assert str(e.value) == "Parameter 'session' should be SQLAlchemy Session." \
                           " Received {}.".format(int)

    with pytest.raises(TypeError) as e:
        Dumm.dumm2(a=11, session="11")
    assert str(e.value) == "Parameter 'session' should be SQLAlchemy Session." \
                           " Received {}.".format(str)

    res = Dumm.dumm2(a=11, session=mock.Mock(spec=Session))
    assert res == 11

    res = Dumm.dumm2(mock.Mock(spec=Session), "11")
    assert res == "11"
