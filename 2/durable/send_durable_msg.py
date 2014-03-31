"""
Simply send durable five messages to queue.
Using pika https://github.com/pika/pika.
"""
import pika

params = pika.ConnectionParameters('localhost')
connection = pika.BlockingConnection(parameters=params)

#make channel
channel = connection.channel()

#create durable queue (will exist after server restart)
channel.queue_declare(queue='sms', durable=True)

# send messages to sms
for i in range(20):
    msg = 'Durable world!{0}'.format(' second' * i)
    channel.basic_publish(exchange='',
                          routing_key='sms',
                          body=msg,
                          properties=pika.BasicProperties(
                              delivery_mode=2,  # make message persistent
                          ))

connection.close()