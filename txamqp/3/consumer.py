#!/usr/bin/env python
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, returnValue

from switch.broker.example_txamqp.helper import connected_amq_client

SEVERITIES = ('info', 'warning', 'debug')


@inlineCallbacks
def consume3_error(result, topics):
    connection, channel = result
    QUEUE_NAME = 'exchange_direct_3_error'

    yield channel.exchange_declare(exchange='exchange_direct_log',
                                   type='direct')

    reply = yield channel.queue_declare(queue=QUEUE_NAME)

    channel.queue_bind(exchange='exchange_direct_log',
                       queue=reply.queue,
                       routing_key='notification.error')

    print ' [*] Waiting for error'

    yield channel.basic_consume(queue=reply.queue,
                                no_ack=True,
                                consumer_tag='consumer3_error')
    queue = yield connection.queue('consumer3_error')

    while True:
        msg = yield queue.get()
        print msg.content.body, 'ERORR'
    returnValue(result)


@inlineCallbacks
def consume3_warning(result, topics):
    connection, channel = result
    QUEUE_NAME = 'exchange_direct_3_warning'

    yield channel.exchange_declare(exchange='exchange_direct_log',
                                   type='direct')

    reply = yield channel.queue_declare(queue=QUEUE_NAME)

    channel.queue_bind(exchange='exchange_direct_log',
                       queue=reply.queue,
                       routing_key='notification.warning')

    print ' [*] Waiting for warnings.'

    yield channel.basic_consume(queue=reply.queue,
                                no_ack=True,
                                consumer_tag='consumer3_warning')
    queue = yield connection.queue('consumer3_warning')

    while True:
        msg = yield queue.get()
        print msg.content.body, 'WARNING'
    returnValue(result)


def main():
    err_def = connected_amq_client()
    war_def = connected_amq_client()

    # consume
    err_def.addCallback(consume3_error, '')
    war_def.addCallback(consume3_warning, '')

    reactor.run()

if __name__ == "__main__":
    main()
