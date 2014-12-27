#! /usr/bin/env python


from celery import Celery

app = Celery('tasks', broker='amqp://guest@localhost//')

app.conf.update(
    BROKER_URL = 'amqp://guest:guest@localhost:5672//',
    CELERY_TASK_SERIALIZER='json',
)

@app.task()
def add(x, y):
    """ Golang task
    """
    pass

@app.task()
def two(d, key="value"):
    """ Golang task
    """
    pass

if __name__ == "__main__":
    add.delay(2, 3)
    two.delay(2)
    two.delay(None, key="foo")
