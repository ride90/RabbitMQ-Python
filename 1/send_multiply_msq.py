"""
Simply send multiply messages to queue.
Using pika https://github.com/pika/pika.
"""
import pika

params = pika.ConnectionParameters('localhost')
connection = pika.BlockingConnection(parameters=params)

#make channel
channel = connection.channel()

#create queue
channel.queue_declare(queue='sms')


for i in xrange(20):
    # send message to sms queue
    channel.basic_publish(exchange='',
                          routing_key='sms',
                          body='Hello {0} World!'.format(i))
    print 'Hello {0} World!'.format(i)

connection.close()


