"""
Simply receive message from queue.
Using pika https://github.com/pika/pika.
"""
import pika


def callback(channel, method, properties, body):
    print " [x] Received {0}".format(body)

params = pika.ConnectionParameters('localhost')
connection = pika.BlockingConnection(parameters=params)

#make channel
channel = connection.channel()

#create or get existing queue
channel.queue_declare(queue='sms')


channel.basic_consume(callback,
                      queue='sms',
                      no_ack=True)

print ' [*] Waiting for messages. To exit press CTRL+C'
channel.start_consuming()


