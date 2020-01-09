Example for usage of taks_manager package
=========================================

# Usage
1. install kafka, zookeeper, and create the topic `example_topic` with 2 partitions
2. put the database uri to `DevelopmentConfig.SQLALCHEMY_DATABASE_URI` in `core/config.py`
3. open 3 terminals
    - in the first one execute `python worker1.py`
    - in the second one execute `python worker2.py`
    - in the third one execute `python manage.py runserver`