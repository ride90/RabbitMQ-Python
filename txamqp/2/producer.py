#!/usr/bin/env python
import cPickle as pickle

from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, returnValue
from txamqp.content import Content

from switch.broker.example_txamqp.helper import connected_amq_client

MESSAGE = "HELLO WORLDS"
COUNT = 10
QUEUE_NAME = 'exchange_fanout_2'


class Chubaka(object):
    def __init__(self, message, num):
        self.num = num
        self.message = message

    def __str__(self):
        return self.message


@inlineCallbacks
def publish2(result, message, count_):
    connection, channel = result

    yield channel.exchange_declare(exchange='log',
                                   type='fanout')

    for i in range(count_):
        msg = Content('%s [%04d]' % (message, i,))
        yield channel.basic_publish(exchange='log',
                                    routing_key='',
                                    content=msg)

        print ' [x] Sent "%s [%04d]"' % (message, i,)
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
    d.addCallback(publish2, MESSAGE, COUNT)
    d.addCallback(publisher_cleanup)

    reactor.run()

if __name__ == "__main__":
    main()




