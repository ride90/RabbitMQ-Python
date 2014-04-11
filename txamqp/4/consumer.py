#!/usr/bin/env python
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, returnValue

from switch.broker.example_txamqp.helper import connected_amq_client

SEVERITIES = ('info', 'warning', 'debug')


@inlineCallbacks
def consume4_error(result):
    connection, channel = result
    QUEUE_NAME = 'exchange_topic_4_error'

    yield channel.exchange_declare(exchange='exchange_topic_log',
                                   type='topic')

    reply = yield channel.queue_declare(queue=QUEUE_NAME)

    channel.queue_bind(exchange='exchange_topic_log',
                       queue=reply.queue,
                       routing_key='*.error')

    print ' [*] Waiting for error'

    yield channel.basic_consume(queue=reply.queue,
                                no_ack=True,
                                consumer_tag='consumer4_error')
    queue = yield connection.queue('consumer4_error')

    while True:
        msg = yield queue.get()
        print msg.content.body, 'ERORR'
    returnValue(result)


@inlineCallbacks
def consume4_warning(result):
    connection, channel = result
    QUEUE_NAME = 'exchange_topic_4_warning'

    yield channel.exchange_declare(exchange='exchange_topic_log',
                                   type='topic')

    reply = yield channel.queue_declare(queue=QUEUE_NAME)

    channel.queue_bind(exchange='exchange_topic_log',
                       queue=reply.queue,
                       routing_key='*.warning')

    print ' [*] Waiting for warnings.'

    yield channel.basic_consume(queue=reply.queue,
                                no_ack=True,
                                consumer_tag='consumer4_warning')
    queue = yield connection.queue('consumer4_warning')

    while True:
        msg = yield queue.get()
        print msg.content.body, 'WARNING'
    returnValue(result)


@inlineCallbacks
def consume4_info(result):
    connection, channel = result
    QUEUE_NAME = 'exchange_topic_4_info'

    yield channel.exchange_declare(exchange='exchange_topic_log',
                                   type='topic')

    reply = yield channel.queue_declare(queue=QUEUE_NAME)

    channel.queue_bind(exchange='exchange_topic_log',
                       queue=reply.queue,
                       routing_key='*.info')

    print ' [*] Waiting for info.'

    yield channel.basic_consume(queue=reply.queue,
                                no_ack=True,
                                consumer_tag='consumer4_info')
    queue = yield connection.queue('consumer4_info')

    while True:
        msg = yield queue.get()
        print msg.content.body, 'INFO'
    returnValue(result)


def main():
    err_def = connected_amq_client()
    war_def = connected_amq_client()
    war_info = connected_amq_client()

    # consume
    err_def.addCallback(consume4_error)
    war_def.addCallback(consume4_warning)
    war_info.addCallback(consume4_info)

    reactor.run()

if __name__ == "__main__":
    main()
