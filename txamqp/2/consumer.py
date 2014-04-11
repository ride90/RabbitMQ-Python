#!/usr/bin/env python
import cPickle as pickle

from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, returnValue

from switch.broker.example_txamqp.helper import connected_amq_client

QUEUE_NAME = 'exchange_fanout_2'


class Chubaka(object):
    def __init__(self, message, num):
        self.num = num
        self.message = message

    def __str__(self):
        return self.message


@inlineCallbacks
def consume2(result):
    connection, channel = result

    reply = yield channel.queue_declare(queue=QUEUE_NAME)

    channel.queue_bind(exchange='log',
                       queue=reply.queue)

    print ' [*] Waiting for messages. To exit press CTRL+C'

    yield channel.basic_consume(queue=reply.queue,
                                no_ack=True,
                                consumer_tag='log_consumer')
    queue = yield connection.queue('log_consumer')

    while True:
        msg = yield queue.get()
        print msg.content.body
    returnValue(result)


def main():
    d = connected_amq_client()
    d.addCallback(consume2)

    reactor.run()

if __name__ == "__main__":
    main()

