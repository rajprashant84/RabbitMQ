__author__ = 'Sefki Kolozali'

from rabbitmq import RabbitMQ
import time


def hello_world():
    # establish connection
    # host = "192.168.1.105"
    # port = 5672
    host = "localhost"
    port = 15672
    rabbitmqconnection, rabbitmqchannel = RabbitMQ.establishConnection(host, port)

    # declare exchange
    exchange = 'Annotated_data'
    topic1 = 'Aarhus.Traffic.3'
    topic2 = 'Aarhus.Traffic.2'
    RabbitMQ.declareExchange(rabbitmqchannel, exchange, _type="topic")

    # send i messages
    i = 10
    for j in range(i):
        message = "hello %i IoT world!" % j
        RabbitMQ.sendMessage(message, rabbitmqchannel, exchange, topic1)
        message = "goodbye %i IoT world!" % j
        RabbitMQ.sendMessage(message, rabbitmqchannel, exchange, topic2)
        print("sent message %i" % j)
        time.sleep(10)

if __name__ == '__main__':
    hello_world()