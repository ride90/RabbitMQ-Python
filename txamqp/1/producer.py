#!/usr/bin/env python
import cPickle as pickle

from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, returnValue
from txamqp.content import Content

from switch.broker.example_txamqp.helper import connected_amq_client

MESSAGE = "HELLO WORLDS"
COUNT = 10
QUEUE_NAME = 'publish1_queue'


class Chubaka(object):
    def __init__(self, message, num):
        self.num = num
        self.message = message

    def __str__(self):
        return self.message


@inlineCallbacks
def publish1(result, message, count_):
    connection, channel = result

    yield channel.queue_declare(queue=QUEUE_NAME, durable=True)
    for i in range(count_):
        msg = Content(pickle.dumps(Chubaka(message, i)))
        msg['delivery mode'] = 2

        yield channel.basic_publish(exchange='',
                                    routing_key=QUEUE_NAME,
                                    content=msg)

        print ' [x] Sent "%s [%04d]"' % (message, i)
    returnValue(result)


@inlineCallbacks
def publisher_cleanup(result):
    #import pdb; pdb.set_trace()
    connection, channel = result

    #yield channel.queue_delete(queue=QUEUE_NAME)

    yield channel.channel_close()
    chan0 = yield connection.channel(0)
    yield chan0.connection_close()
    reactor.stop()


def main():
    d = connected_amq_client()

    # publish something
    d.addCallback(publish1, MESSAGE, COUNT)
    d.addCallback(publisher_cleanup)

    reactor.run()

if __name__ == "__main__":
    main()


