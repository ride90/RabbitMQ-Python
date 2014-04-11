#!/usr/bin/env python

import sys
import uuid
import cPickle as pickle

from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet.protocol import ClientCreator

from txamqp.client import TwistedDelegate
from txamqp.content import Content
from txamqp.protocol import AMQClient
import txamqp.spec

import settings

NUM_MSGS = 1
DEFAULT_TOPICS = 'info'
QUEUE_NAME = 'txamqp_example_queue'
ROUTING_TOPIC = 'txamqp_example_topic'
EXCHANGE_NAME = 'txamqp_example_exchange'
SEVERITIES = ('info', 'warning', 'debug')
FACILITIES = ('kern', 'mail')


@inlineCallbacks
def consume1(result, topics):
    connection, channel = result
    QUEUE_NAME = 'publish1_queue'

    yield channel.queue_declare(queue=QUEUE_NAME, durable=True)

    msgnum = 0
    print ' [*] Waiting for messages. To exit press CTRL+C'

    yield channel.basic_consume(queue=QUEUE_NAME,
                                consumer_tag='qtag')

    queue = yield connection.queue('qtag')

    while True:
        msg = yield queue.get()
        msgnum += 1
        print " [%04d] Received %r from channel #%d" % (msgnum,
                                                        pickle.loads(msg.content.body),
                                                        channel.id)
        channel.basic_ack(delivery_tag=msg.delivery_tag)


    returnValue(result)

@inlineCallbacks
def consume2(result, topics):
    connection, channel = result
    yield channel.queue_declare(queue=QUEUE_NAME, durable=True)
    msgnum = 0
    print ' [*] Waiting for messages. To exit press CTRL+C'
    yield channel.basic_qos(prefetch_count=1)
    yield channel.basic_consume(queue=QUEUE_NAME, consumer_tag='qtag')
    queue = yield connection.queue('qtag')
    while True:
        msg = yield queue.get()
        msgnum += 1
        print " [%04d] Received %r from channel #%d" % (
            msgnum, msg.content.body, channel.id)
        channel.basic_ack(delivery_tag=msg.delivery_tag)
    returnValue(result)


@inlineCallbacks
def consume3(result, topics):
    connection, channel = result
    EXCHANGE_NAME = 'log'

    yield channel.exchange_declare(exchange=EXCHANGE_NAME,
                                   type='fanout')

    reply = yield channel.queue_declare(exclusive=True)

    channel.queue_bind(exchange=EXCHANGE_NAME,
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


@inlineCallbacks
def consume4(result, topics):
    connection, channel = result
    exchange_name = '%s_4' % EXCHANGE_NAME
    yield channel.exchange_declare(exchange=exchange_name, type='direct')
    reply = yield channel.queue_declare(exclusive=True)
    queue_name = reply.queue
    msgnum = 0
    severities = topics.split(',')
    for severity in severities:
        channel.queue_bind(
            exchange=exchange_name, queue=queue_name, routing_key=severity)
    print ' [*] Waiting for messages. To exit press CTRL+C'
    yield channel.basic_consume(
        queue=queue_name, no_ack=True, consumer_tag='qtag')
    queue = yield connection.queue('qtag')
    while True:
        msg = yield queue.get()
        msgnum += 1
        print " [%04d] Received %r from channel #%d with severity [%s]" % (
            msgnum, msg.content.body, channel.id, msg.routing_key)
    returnValue(result)


@inlineCallbacks
def consume5(result, topics):
    connection, channel = result
    exchange_name = '%s_5' % EXCHANGE_NAME
    yield channel.exchange_declare(exchange=exchange_name, type='topic')
    reply = yield channel.queue_declare(exclusive=True)
    queue_name = reply.queue
    msgnum = 0
    routing_keys = topics.split(',')
    for routing_key in routing_keys:
        channel.queue_bind(
            exchange=exchange_name, queue=queue_name, routing_key=routing_key)
    print ' [*] Waiting for messages. To exit press CTRL+C'
    yield channel.basic_consume(
        queue=queue_name, no_ack=True, consumer_tag='qtag')
    queue = yield connection.queue('qtag')
    while True:
        msg = yield queue.get()
        msgnum += 1
        print (' [%04d] Received %r from channel #%d '
            'with facility and severity [%s]') % (
            msgnum, msg.content.body, channel.id, msg.routing_key)
    returnValue(result)

@inlineCallbacks
def consume6(result, topics):
    connection, channel = result

    QUEUE_NAME = 'publish6_unique_queue'

    yield channel.queue_declare(queue=QUEUE_NAME)

    msgnum = 0
    print ' [*] Waiting for RPC requests. To exit press CTRL+C'

    yield channel.basic_qos(prefetch_count=1)
    yield channel.basic_consume(queue=QUEUE_NAME,
                                no_ack=True,
                                consumer_tag='qtag')

    queue = yield connection.queue('qtag')

    def fib(n):
        if n == 0:
            return 0
        elif n == 1:
            return 1
        else:
            return fib(n-1) + fib(n-2)

    while True:
        msg = yield queue.get()
        input_ = msg.content.body
        properties = msg.content.properties
        msgnum += 1
        print ' [%04d] Received fib(%r) from channel #%d' % (
            msgnum, input_, channel.id)
        output = fib(int(input_))
        response = Content(str(output))
        response['correlation id'] = properties['correlation id']
        channel.basic_publish(exchange='', routing_key=properties['reply to'], content=response)
    returnValue(result)

########################## PUBLISHERS ########################
@inlineCallbacks
def publish1(result, message, count_):
    connection, channel = result

    QUEUE_NAME = 'publish1_queue'

    yield channel.queue_declare(queue=QUEUE_NAME, durable=True)
    for i in range(count_):
        #msg = Content('{0} [{1}]'.format(message, i))
        msg = Content(pickle.dumps(Chubaka(message, i)))
        msg['delivery mode'] = 2

        print message, msg

        yield channel.basic_publish(exchange='',
                                    routing_key=QUEUE_NAME,
                                    content=msg)

        print ' [x] Sent "%s [%04d]"' % (message, i)
    returnValue(result)


@inlineCallbacks
def publish3(result, message, count_):
    connection, channel = result
    EXCHANGE_NAME = 'log'

    yield channel.exchange_declare(exchange=EXCHANGE_NAME,
                                   type='fanout')

    for i in range(count_):
        msg = Content('%s [%04d]' % (message, i,))
        yield channel.basic_publish(exchange=EXCHANGE_NAME,
                                    routing_key='',
                                    content=msg)

        print ' [x] Sent "%s [%04d]"' % (message, i,)
    returnValue(result)

@inlineCallbacks
def publish4(result, message, count_):
    connection, channel = result
    exchange_name = '%s_4' % EXCHANGE_NAME
    yield channel.exchange_declare(exchange=exchange_name, type='direct')
    for i in range(count_):
        msg = Content('%s [%04d]' % (message, i,))
        severity = SEVERITIES[i % len(SEVERITIES)]
        yield channel.basic_publish(
            exchange=exchange_name, routing_key=severity, content=msg)
        print ' [x] Sent "%s [%04d]" with severity [%s]' % (
            message, i, severity,)
    returnValue(result)

@inlineCallbacks
def publish5(result, message, count_):
    connection, channel = result
    exchange_name = '%s_5' % EXCHANGE_NAME
    yield channel.exchange_declare(exchange=exchange_name, type='topic')
    for i in range(count_):
        msg = Content('%s [%04d]' % (message, i))
        severity = SEVERITIES[i % len(SEVERITIES)]
        facility = FACILITIES[i % len(FACILITIES)]
        routing_key = '%s.%s' % (facility, severity)
        yield channel.basic_publish(
            exchange=exchange_name, routing_key=routing_key, content=msg)
        print ' [x] Sent "%s [%04d]" with facility and severity [%s]' % (
            message, i, routing_key)
    returnValue(result)


@inlineCallbacks
def publish6(result, message, count_):
    connection, channel = result

    QUEUE_NAME = 'publish6_unique_queue'

    @inlineCallbacks
    def call(n):
        corr_id = str(uuid.uuid4())

        reply = yield channel.queue_declare(exclusive=True)
        callback_queue = reply.queue

        msg = Content(str(n))
        msg['correlation id'] = corr_id
        msg['reply to'] = callback_queue

        yield channel.basic_publish(exchange='',
                                    routing_key=QUEUE_NAME,
                                    content=msg)

        yield channel.basic_consume(queue=callback_queue,
                                    no_ack=True,
                                    consumer_tag='qtag')

        queue = yield connection.queue('qtag')

        while True:
            response = yield queue.get()
            if response.content.properties['correlation id'] == corr_id:
                returnValue(response.content.body)

    response = yield call(message)
    print 'Got', response
    returnValue(result)


SPECS = {}


def get_spec(specfile):
    """
    Cache the generated part of txamqp, because generating it is expensive.

    This is important for tests, which create lots of txamqp clients,
    and therefore generate lots of specs. Just doing this results in a
    decidedly happy test run time reduction.
    """
    if specfile not in SPECS:
        SPECS[specfile] = txamqp.spec.load(specfile)
    return SPECS[specfile]


@inlineCallbacks
def gotConnection(connection, username, password):
    print ' Got connection, authenticating.'
    # Older version of AMQClient have no authenticate
    # yield connection.authenticate(username, password)
    # This produces the same effect

    yield connection.start({'LOGIN': username, 'PASSWORD': password})

    channel = yield connection.channel(1)
    yield channel.channel_open()
    print ' Channel was opened'

    returnValue((connection, channel))


class Chubaka(object):
    def __init__(self, message, num):
        self.num = num
        self.message = message

    def __str__(self):
        return self.message


def setup_queue_cleanup(result):
    """Executed in consumers on Ctrl-C, but not in publishers."""
    connection, channel = result
    reactor.addSystemEventTrigger('before', 'shutdown',
        consumer_cleanup, channel)
    return result


def consumer_cleanup(channel):
    return channel.queue_delete(QUEUE_NAME)


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
    PORT = 5672
    MESSAGE = "Hello publixh 1"
    COUNT = 10
    TOPICS = 'topic_name'

    HOST = 'localhost'
    VHOST = '/'
    USERNAME = 'guest'
    PASSWORD = 'guest'
    spec_path = '{0}/switch/broker/specs/amqp0-8.rabbitmq.xml'.format(
        settings.PROJECT_ROOT)
    SPEC = get_spec(spec_path)

    delegate = TwistedDelegate()

    d = ClientCreator(reactor, AMQClient, delegate=delegate, vhost=VHOST,
                      spec=SPEC).connectTCP(HOST, PORT)

    # auth and get channel
    d.addCallback(gotConnection, USERNAME, PASSWORD)

    # publish something
    d.addCallback(publish3, MESSAGE, COUNT)

    # consume
    #d.addCallback(consume3, TOPICS)

    d.addCallback(publisher_cleanup)

    reactor.run()

if __name__ == "__main__":
    main()