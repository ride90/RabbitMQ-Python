import sys, uuid

from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet.protocol import ClientCreator

from txamqp.client import TwistedDelegate
from txamqp.content import Content
from txamqp.protocol import AMQClient
import txamqp.spec

from utils.log import debug
import settings


SPECS_DICT = {}
PORT = 5672
HOST = 'localhost'
VHOST = '/'
USERNAME = 'smpp'
PASSWORD = 'smpp'


def get_spec(specfile):
    """
    Cache the generated part of txamqp, because generating it is expensive.

    This is important for tests, which create lots of txamqp clients,
    and therefore generate lots of specs. Just doing this results in a
    decidedly happy test run time reduction.
    """
    if specfile not in SPECS_DICT:
        SPECS_DICT[specfile] = txamqp.spec.load(specfile)
    return SPECS_DICT[specfile]


spec_path = '{0}/switch/broker/specs/amqp0-8.rabbitmq.xml'.format(
    settings.PROJECT_ROOT)
SPEC = get_spec(spec_path)


@inlineCallbacks
def got_connection(connection, username, password):
    debug('RABBIT-AMQ: Ready to connect.')
    # Older version of AMQClient have no authenticate
    # yield connection.authenticate(username, password)
    # This produces the same effect

    yield connection.start({'LOGIN': username, 'PASSWORD': password})
    debug('RABBIT-AMQ: Connection started.')

    channel = yield connection.channel(1)
    yield channel.channel_open()
    debug('RABBIT-AMQ: Channel was opened => {0}'.format(channel))

    returnValue((connection, channel))


@inlineCallbacks
def publisher_cleanup(connection):
    #import pdb; pdb.set_trace()
    protocol, channel = connection
    #yield channel.queue_delete(queue=QUEUE_NAME)

    yield channel.channel_close()
    chan0 = yield protocol.channel(0)
    yield chan0.connection_close()


@inlineCallbacks
def connected_amq_client():
    """
    Return connected AMQ protocol and channel
    """
    protocol = yield ClientCreator(reactor, AMQClient,
                                     delegate=TwistedDelegate(),
                                     vhost=VHOST, spec=SPEC).connectTCP(HOST,
                                                                        PORT)
    # auth
    debug('RABBIT-AMQ: Ready to connect.')
    # Older version of AMQClient have no authenticate
    # yield connection.authenticate(username, password)
    # This produces the same effect
    yield protocol.start({'LOGIN': USERNAME, 'PASSWORD': PASSWORD})
    debug('RABBIT-AMQ: Connection started.')

    # get channel
    channel = yield protocol.channel(1)
    yield channel.channel_open()
    debug('RABBIT-AMQ: Channel was opened => {0}'.format(channel))

    returnValue((protocol, channel))
