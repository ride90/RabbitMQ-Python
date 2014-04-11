#!/usr/bin/env python
import cPickle as pickle

from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, returnValue

from switch.broker.example_txamqp.helper import connected_amq_client

QUEUE_NAME = 'publish1_queue'


class Chubaka(object):
    def __init__(self, message, num):
        self.num = num
        self.message = message

    def __str__(self):
        return self.message


@inlineCallbacks
def consume1(result):
    connection, channel = result

    yield channel.queue_declare(queue=QUEUE_NAME, durable=True)

    msgnum = 0
    print ' [*] Waiting for messages. To exit press CTRL+C!'

    yield channel.basic_consume(queue=QUEUE_NAME,
                                consumer_tag='consume1')

    queue = yield connection.queue('consume1')

    while True:
        msg = yield queue.get()
        msgnum += 1
        print " [%04d] Received %r from channel #%d" % (msgnum,
                                                        pickle.loads(msg.content.body),
                                                        channel.id)
        channel.basic_ack(delivery_tag=msg.delivery_tag)

    returnValue(result)


def main():
    d = connected_amq_client()
    d.addCallback(consume1)

    reactor.run()

if __name__ == "__main__":
    main()
