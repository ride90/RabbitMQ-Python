"""
Worker which simulate hard work. Work with durable queue.
Using pika https://github.com/pika/pika.
"""
import time
import pika


def callback(channel, method, properties, body):
    print " [x] Received {0}".format(body)
    time.sleep(float(body.count('second'))/20)
    print " [x] Done"
    channel.basic_ack(delivery_tag=method.delivery_tag)

params = pika.ConnectionParameters('localhost')
connection = pika.BlockingConnection(parameters=params)

#make channel
channel = connection.channel()

#create or get existing queue
channel.queue_declare(queue='sms', durable=True)


channel.basic_qos(prefetch_count=1)
channel.basic_consume(callback,
                      queue='sms')

print ' [*] Waiting for messages. To exit press CTRL+C'
channel.start_consuming()

