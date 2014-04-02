"""
Simply send one message to queue.
Using pika https://github.com/pika/pika.
"""
import pika

params = pika.ConnectionParameters('localhost')
connection = pika.BlockingConnection(parameters=params)

#make channel
channel = connection.channel()

#create queue
channel.queue_declare(queue='sms')

# send message to sms
channel.basic_publish(exchange='',
                      routing_key='sms',
                      body='Hello World!')

print " [x] Sent 'Hello World!'"

connection.close()


