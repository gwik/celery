#! /usr/bin/env python


from kombu import Exchange, Queue
from celery import Celery
from kombu import Exchange

app = Celery('tasks', broker='amqp://guest@localhost//')

media_exchange = Exchange('notifications', type='direct')

app.conf.update(
    BROKER_URL = 'amqp://guest:guest@localhost:5672//',
    CELERY_TASK_SERIALIZER='json',
    CELERY_RESULT_SERIALIZER='json',
    CELERY_RESULT_BACKEND= 'amqp://guest:guest@localhost:5672//',
    CELERY_QUEUES = (
        Queue('celery', routing_key='celery', delivery_mode=1),),
)

import pprint
pprint.pprint(app.conf.keys())


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

@app.task()
def unknown():
    pass

@app.task()
def tryagain(delay):
    pass


@app.task()
def byname(foo, bar):
    pass

@app.task()
def panic(msg):
    pass

@app.task(ignore_result=True)
def notify_user(userid, data):
    pass


def notify(userid, data):
    notify(args=[data], queue=user_id, exchange="notifications")



if __name__ == "__main__":



    def ex1():
        # for i in xrange(200):
        #     unknown.delay()

        two.apply_async(args=["LAST"], countdown=10)
        two.apply_async(args=["LATER"], countdown=2)

        for i in xrange(10):
            two.apply_async(args=["BULK"], countdown=10)

        for i in xrange(10000):
            add.delay(2, 3)
            two.delay(2)

        unknown.delay()

        two.delay(None, key="foo")
        two.apply_async(args=["LATER"], countdown=2)

        unknown.delay()

    def ex_result():
        result = add.delay(2, 3)
        print result.get()

    def retry():
        tryagain.delay(0.1, 100)
        tryagain.delay(10, 3)

    def name():
        byname.delay("foo", 10)
        panic.delay("panic")
        panic.delay("panic", "wrong number of arguments")

    # ex_result()
    # ex1()
    retry()
    # for i in range(1000):
    #     notify(i)

    # add.delay(2, 3)



