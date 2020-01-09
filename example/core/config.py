"""
Configuration file for Labeling App.
To run the app you have to define 4 environment variables:
    -  SQLALCHEMY_DATABASE_URI_PROD
    -  SECRET_KEY_PROD
    -  SQLALCHEMY_DATABASE_URI_DEV
    -  SECRET_KEY_DEV
If you just want to use the development configuration, you can set the
values of variables 'SQLALCHEMY_DATABASE_URI_PROD' and 'SECRET_KEY_PROD' to
empty string.
"""
import datetime as dt


class Config(object):
    """
    General configurations for all the environments
    """
    DEBUG = False
    TESTING = False
    SQLALCHEMY_DATABASE_URI = 'sqlite:///:memory:'
    SQLALCHEMY_TRACK_MODIFICATIONS = True
    REMEMBER_COOKIE_NAME = 'taskmanager'
    REMEMBER_COOKIE_DURATION = dt.timedelta(days=31)
    PROPAGATE_EXCEPTIONS = True
    WTF_CSRF_SECRET_KEY = '+8yjG#Cp&!GEZ-cK4WNYxrLNQ'


class DevelopmentConfig(Config):
    """
    Special configuration for the Development environment.
    The SECRET_KET in Development should be different to the one in Production.
    """
    DEBUG = True
    SQLALCHEMY_DATABASE_URI = "postgres://taskmanager:123456@localhost:5432/testdb"
    SECRET_KEY = '27gRERC0%={}ONhHL/h$vd~9b'


class TestingConfig(Config):
    """
    Configuration for testing
    """
    TESTING = True
