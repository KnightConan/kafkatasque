__all__ = ['json_encode', 'json_decode', 'validate_uuid4', 'ensure_int',
           'repr_func', 'is_optional', 'is_optional_type',
           'validated_setter_property', 'session_required']

import jsonpickle
from functools import wraps
from uuid import UUID
from inspect import ismethod, isfunction, isbuiltin, isclass
from typing import Any, Optional, Union, Callable
from sqlalchemy.orm.session import Session


def is_optional(value: Any,
                validation: bool) -> bool:
    """Check if the given value is None or satisfy the validation condition.

    :param value: value to check
    :type value: object
    :param validation: validation conditions.
    :type validation: bool
    :return: True, if the value is None or satisfy the validation condition.
    :rtype: bool
    """
    return value is None or validation


def is_optional_type(value: Any,
                     datatype: Any,
                     extra_check: Optional[bool] = None) -> bool:
    """Check if the given value is None or is the given datatype and satisfy the
     extra_check condition.

    :param value: value to check.
    :type value: object
    :param datatype: expected date type, if the value is not None
    :type datatype: object
    :param extra_check: extra check condition.
    :type extra_check: bool | None
    :return: True, if the value is None or it is the given datatype and
        satisfies the extra_check condition.
    :rtype: bool
    """
    validation = isinstance(value, datatype)
    if extra_check is not None:
        validation = validation and extra_check
    return is_optional(value, validation)


def repr_func(func: Callable,
              *args: Any,
              **kwargs: Any) -> str:
    """Return the string representation of the function call.

    :param func: A callable function.
    :type func: callable
    :param args: positional arguments for the callable function.
    :param kwargs: keyword arguments for the callable function.
    :return: string representation of the function call.
    :rtype: str
    """
    # Functions, builtins and methods
    if ismethod(func) or isfunction(func) or isbuiltin(func):
        func_repr = '{}.{}'.format(func.__module__, func.__qualname__)
    # A callable class instance
    elif not isclass(func) and hasattr(func, '__call__'):
        func_repr = '{}.{}'.format(func.__module__, func.__class__.__name__)
    else:
        func_repr = repr(func)

    args_reprs = [repr(arg) for arg in args]
    kwargs_reprs = [k + '=' + repr(v) for k, v in sorted(kwargs.items())]
    return '{}({})'.format(func_repr, ', '.join(args_reprs + kwargs_reprs))


def json_encode(obj: Any,
                to_bytes: bool = True) -> Union[str, bytes, None]:
    """Encode the given python object to string/bytes.

    :param obj: python object to encode
    :type obj: python object
    :param to_bytes: if True, then convert the given object to bytes; else to
        string.
    :type to_bytes: bool
    :return: encoded string or bytes, or error raised
    :rtype: str | bytes | None
    """
    encoded_str = jsonpickle.encode(obj)
    return encoded_str.encode('gbk') if to_bytes else encoded_str


def json_decode(obj: Union[str, bytes]) -> Any:
    """Decode the given string/bytes to object.

    :param obj: string to decode
    :type obj: str/bytes
    :return: decoded object
    :rtype: object
    """
    if isinstance(obj, bytes):
        encoded_str = obj.decode('gbk')
    elif isinstance(obj, str):
        encoded_str = obj
    else:
        raise ValueError("Invalid input, expected bytes or string."
                         " Received [{}]".format(type(obj)))
    return jsonpickle.decode(encoded_str)


def validate_uuid4(uuid_hex_str: str) -> bool:
    """Validate that the given uuid_hex_str is a valid uuid4 hex string.

    :param uuid_hex_str: uuid hex string to be validated
    :type uuid_hex_str: str
    :return: True, if valid.
    :rtype: bool
    """
    if not isinstance(uuid_hex_str, str):
        return False
    try:
        val = UUID(uuid_hex_str, version=4)
    except ValueError:
        # If it's a value error, then the string
        # is not a valid hex code for a UUID.
        return False
    # If the uuid_hex_str is a valid hex code, but an invalid uuid4,
    # the UUID.__init__ will convert it to a valid uuid4.
    # so we need to make sure that the hex value is not changed.
    return val.hex == uuid_hex_str


def ensure_int(value: Union[str, float],
               default_value: int = 0) -> Optional[int]:
    """Ensure the given value is converted to int.

    :param value: value to convert
    :type value: str | float
    :param default_value: default value, if unable to convert it.
    :type default_value: int
    :return: converted int value
    :rtype: int | None
    """
    if isinstance(value, int):
        return value
    try:
        int_value = int(float(value))
    except Exception as e:
        return default_value
    return int_value


class validated_setter_property:
    """Descriptor for enforcing the variable type check.

    :param func: function to do the type check
    :type func: Callable
    :param name: variable name
    :type name: None | str
    :param doc: variable doc
    :type doc: int| None
    """
    def __init__(self, func, name=None, doc=None):
        self.func = func
        self.__name__ = name or func.__name__
        self.__doc__ = doc or func.__doc__

    def __set__(self, obj, value):
        """Check the type and set the value

        :param obj: class instance
        :type obj: class instance
        :param value: new value of the variable in class instance
        :type value: any
        :return: None
        :rtype: None
        """
        self.func(obj, value)
        obj.__dict__[self.__name__] = value


def session_required(func):
    """Decorator for checking the type of the parameter 'session' ina function
    call.

    This decorator assumes that the parameter 'session' is the keyword arguments
    or the first item of the positional arguments of the function.
    """
    @wraps(func)
    def decorated(*args, **kwargs):
        if "session" in kwargs:
            session = kwargs["session"]
        # if the qualname not equal to the name, it means func is a function
        # inside class
        elif func.__qualname__ != func.__name__:
            session = args[1]
        else:
            session = args[0]
        if not isinstance(session, Session):
            raise TypeError("Parameter 'session' should be SQLAlchemy Session."
                            " Received {}.".format(type(session)))
        return func(*args, **kwargs)
    return decorated
