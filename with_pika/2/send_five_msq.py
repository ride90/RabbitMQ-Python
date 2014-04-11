"""
Simply send five messages to queue.
Using pika https://github.com/pika/pika.
"""
import pika

params = pika.ConnectionParameters('localhost')
connection = pika.BlockingConnection(parameters=params)

#make channel
channel = connection.channel()

#create queue
channel.queue_declare(queue='sms')

# send messages to sms
msg = 'Hello World!'
channel.basic_publish(exchange='',
                      routing_key='sms',
                      body=msg)
print " [x] Sent {0}".format(msg)

msg = 'Hello World! second'
channel.basic_publish(exchange='',
                      routing_key='sms',
                      body=msg)
print " [x] Sent {0}".format(msg)

msg = 'Hello World! second second'
channel.basic_publish(exchange='',
                      routing_key='sms',
                      body=msg)
print " [x] Sent {0}".format(msg)

msg = 'Hello World! second second second'
channel.basic_publish(exchange='',
                      routing_key='sms',
                      body=msg)
print " [x] Sent {0}".format(msg)

msg = 'Hello World! second second second second'
channel.basic_publish(exchange='',
                      routing_key='sms',
                      body=msg)
print " [x] Sent {0}".format(msg)

connection.close()