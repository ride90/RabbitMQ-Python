"""
Simply send one message to queue.
Using pika https://github.com/pika/pika.
"""
import pika

params = pika.ConnectionParameters('localhost')
connection = pika.BlockingConnection(parameters=params)

channel = connection.channel()

print dir(channel)

# ['CLOSED', 'CLOSING', 'NO_RESPONSE_FRAMES', 'OPEN', 'OPENING', '__class__',
#  '__delattr__', '__dict__', '__doc__', '__format__', '__getattribute__',
#  '__hash__', '__init__', '__int__', '__module__', '__new__', '__reduce__',
#  '__reduce_ex__', '__repr__', '__setattr__', '__sizeof__', '__str__',
#  '__subclasshook__', '__weakref__', '_add_callbacks', '_add_pending_msg',
#  '_add_reply', '_blocked', '_blocking', '_cancelled', '_cleanup',
#  '_confirmation', '_consumers', '_force_data_events_override', '_frames',
#  '_generator', '_generator_callback', '_generator_messages', '_get_pending_msg',
#  '_handle_content_frame', '_has_content', '_has_on_flow_callback', '_on_cancel',
#  '_on_cancelok', '_on_close', '_on_closeok', '_on_deliver', '_on_eventok',
#  '_on_flow', '_on_flowok', '_on_flowok_callback', '_on_getempty', '_on_getok',
#  '_on_getok_callback', '_on_openok', '_on_openok_callback', '_on_return',
#  '_on_rpc_complete', '_on_selectok', '_on_synchronous_complete', '_pending',
#  '_process_replies', '_received_response', '_remove_reply', '_replies', '_rpc',
#  '_send_method', '_set_state', '_state', '_unexpected_frame',
#  '_validate_acceptable_replies', '_validate_callback',
#  '_validate_channel_and_callback', '_wait', '_wait_on_response', 'add_callback',
#  'add_on_cancel_callback', 'add_on_close_callback', 'add_on_flow_callback',
#  'add_on_return_callback', 'basic_ack', 'basic_cancel', 'basic_consume',
#  'basic_get', 'basic_nack', 'basic_publish', 'basic_qos', 'basic_recover',
#  'basic_reject', 'callbacks', 'cancel', 'channel_number', 'close',
#  'confirm_delivery', 'connection', 'consume', 'consumer_tags', 'exchange_bind',
#  'exchange_declare', 'exchange_delete', 'exchange_unbind', 'flow',
#  'force_data_events', 'frame_dispatcher', 'is_closed', 'is_closing', 'is_open',
#  'open', 'queue_bind', 'queue_declare', 'queue_delete', 'queue_purge',
#  'queue_unbind', 'start_consuming', 'stop_consuming', 'tx_commit',
#  'tx_rollback', 'tx_select', 'wait']


