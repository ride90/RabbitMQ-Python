import optparse
import sys
import uuid

from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet.protocol import ClientCreator
from txamqp.client import TwistedDelegate
from txamqp.content import Content
from txamqp.protocol import AMQClient
import txamqp.spec

NUM_MSGS = 1
DEFAULT_TOPICS = 'info'
QUEUE_NAME = 'txamqp_example_queue'
ROUTING_TOPIC = 'txamqp_example_topic'
EXCHANGE_NAME = 'txamqp_example_exchange'
SEVERITIES = ('info', 'warning', 'debug')
FACILITIES = ('kern', 'mail')


@inlineCallbacks
def gotConnection(connection, username, password):
    print ' Got connection, authenticating.'
    # Older version of AMQClient have no authenticate
    # yield connection.authenticate(username, password)
    # This produces the same effect
    yield connection.start({'LOGIN': username, 'PASSWORD': password})
    channel = yield connection.channel(1)
    yield channel.channel_open()
    returnValue((connection, channel))


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
    #    import pdb; pdb.set_trace()
    connection, channel = result
    #    yield channel.queue_delete(queue=QUEUE_NAME)
    yield channel.channel_close()
    chan0 = yield connection.channel(0)
    yield chan0.connection_close()
    reactor.stop()


@inlineCallbacks
def consume1(result, topics):
    connection, channel = result
    yield channel.queue_declare(queue=QUEUE_NAME)
    msgnum = 0
    print ' [*] Waiting for messages. To exit press CTRL+C'
    yield channel.basic_consume(queue=QUEUE_NAME, no_ack=True,
                                consumer_tag='qtag')
    queue = yield connection.queue('qtag')
    while True:
        msg = yield queue.get()
        msgnum += 1
        print " [%04d] Received %r from channel #%d" % (
        msgnum, msg.content.body, channel.id)
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
    yield channel.exchange_declare(exchange=EXCHANGE_NAME, type='fanout')
    reply = yield channel.queue_declare(exclusive=True)
    queue_name = reply.queue
    msgnum = 0
    channel.queue_bind(exchange=EXCHANGE_NAME, queue=queue_name)
    print ' [*] Waiting for messages. To exit press CTRL+C'
    yield channel.basic_consume(
        queue=queue_name, no_ack=True, consumer_tag='qtag')
    queue = yield connection.queue('qtag')
    while True:
        msg = yield queue.get()
        msgnum += 1
        print " [%04d] Received %r from channel #%d" % (
            msgnum, msg.content.body, channel.id)
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
    yield channel.queue_declare(queue='rpc_queue')
    msgnum = 0
    print ' [*] Waiting for RPC requests. To exit press CTRL+C'
    yield channel.basic_qos(prefetch_count=1)
    yield channel.basic_consume(
        queue='rpc_queue', no_ack=True, consumer_tag='qtag')
    queue = yield connection.queue('qtag')

    def fib(n):
        if n == 0:
            return 0
        elif n == 1:
            return 1
        else:
            return fib(n - 1) + fib(n - 2)

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
        channel.basic_publish(exchange='', routing_key=properties['reply to'],
                              content=response)
    returnValue(result)


@inlineCallbacks
def publish1(result, message, count_):
    connection, channel = result
    yield channel.queue_declare(queue=QUEUE_NAME)
    for i in range(count_):
        msg = Content('%s [%04d]' % (message, i,))
        yield channel.basic_publish(
            exchange='', routing_key=QUEUE_NAME, content=msg)
        print ' [x] Sent "%s [%04d]"' % (message, i,)
    returnValue(result)


@inlineCallbacks
def publish2(result, message, count_):
    connection, channel = result
    yield channel.queue_declare(queue=QUEUE_NAME, durable=True)
    for i in range(count_):
        msg = Content('%s [%04d]' % (message, i,))
        msg['delivery mode'] = 2
        yield channel.basic_publish(
            exchange='', routing_key=QUEUE_NAME, content=msg)
        print ' [x] Sent "%s [%04d]"' % (message, i,)
    returnValue(result)


@inlineCallbacks
def publish3(result, message, count_):
    connection, channel = result
    yield channel.exchange_declare(exchange=EXCHANGE_NAME, type='fanout')
    for i in range(count_):
        msg = Content('%s [%04d]' % (message, i,))
        yield channel.basic_publish(
            exchange=EXCHANGE_NAME, routing_key='', content=msg)
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

    @inlineCallbacks
    def call(n):
        corr_id = str(uuid.uuid4())
        reply = yield channel.queue_declare(exclusive=True)
        callback_queue = reply.queue
        msg = Content(str(n))
        msg['correlation id'] = corr_id
        msg['reply to'] = callback_queue
        yield channel.basic_publish(
            exchange='', routing_key='rpc_queue', content=msg)
        print ' [x] Sent "%s"' % n
        yield channel.basic_consume(
            queue=callback_queue, no_ack=True, consumer_tag='qtag')
        queue = yield connection.queue('qtag')
        while True:
            response = yield queue.get()
            if response.content.properties['correlation id'] == corr_id:
                returnValue(response.content.body)

    print ' [x] Requesting fib(%s)' % message
    response = yield call(message)
    print ' [.] Got %r' % response
    returnValue(result)


PUBLISH_EXAMPLES = {
    'publish1': publish1,
    'publish2': publish2,
    'publish3': publish3,
    'publish4': publish4,
    'publish5': publish5,
    'publish6': publish6,
}

CONSUME_EXAMPLES = {
    'consume1': consume1,
    'consume2': consume2,
    'consume3': consume3,
    'consume4': consume4,
    'consume5': consume5,
    'consume6': consume6,
}

ALL_EXAMPLES = {}
ALL_EXAMPLES.update(PUBLISH_EXAMPLES)
ALL_EXAMPLES.update(CONSUME_EXAMPLES)


def check_cmd_line(options, args, abort):
    """
    Check the command line options and args, and exit with an error message
    if something is not acceptable.
    """
    if args:
        abort('No arguments needed')
    if not 0 < options.port < 65536:
        abort('Please specify a port number between 1 and 65535 included')
    if options.example not in ALL_EXAMPLES.keys():
        abort('Please specify one of the following, as an example name: %s' %
              ALL_EXAMPLES.keys())
    if options.example in PUBLISH_EXAMPLES and options.message is None:
        abort('Please provide a message to publish')
    if options.message is not None and (
        options.example not in PUBLISH_EXAMPLES):
        abort('Setting the message is only meaningful for publish examples')
    if options.count < 1:
        abort('Please set a positive number of messages')
    return (options.port, options.example, options.message, options.count,
            options.topics)


def parse_cmd_line(parser, all_args):
    """
    Parse command line arguments using the optparse library.
    Return (options, args).
    """
    parser.set_usage(
        'Usage: %s -p -e [producer options: -m -c | '
        'consumer options: -t topic1,topic2,...]' % all_args[0])
    all_args = all_args[1:]
    parser.add_option('-p', '--port', dest='port', type='int',
                      help='RabbitMQ server port number')
    parser.add_option('-e', '--example', dest='example',
                      help='example name: "produceX" or "consumeX", with X from 1 to 6')
    parser.add_option('-m', '--message', dest='message',
                      help='message to send as a producer')
    parser.add_option('-c', '--count', dest='count', type='int',
                      help='num. of messages to send as a producer: default: %d' % NUM_MSGS)
    parser.add_option('-t', '--topics', dest='topics',
                      help='topics to subscribe this consumer to')
    parser.set_defaults(count=NUM_MSGS, topics=DEFAULT_TOPICS)
    return parser.parse_args(all_args)


def main(all_args=None):
    """The main"""
    all_args = all_args or []
    parser = optparse.OptionParser()
    options, args = parse_cmd_line(parser, all_args)
    (port, example, message, count_, topics) = check_cmd_line(
        options, args, parser.error)

    host = 'localhost'
    vhost = '/'
    username = 'guest'
    password = 'guest'
    spec = txamqp.spec.load('your_specs_path.xml')

    delegate = TwistedDelegate()

    d = ClientCreator(reactor, AMQClient, delegate=delegate, vhost=vhost,
                      spec=spec).connectTCP(host, port)

    d.addCallback(gotConnection, username, password)
    if example in PUBLISH_EXAMPLES:
        d.addCallback(ALL_EXAMPLES[example], message, count_)
    else:
        d.addCallback(ALL_EXAMPLES[example], topics)
    d.addCallback(publisher_cleanup)
    d.addErrback(lambda f: sys.stderr.write(str(f)))
    reactor.run()


if __name__ == "__main__":
    main(all_args=sys.argv)