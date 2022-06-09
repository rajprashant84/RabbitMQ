__author__ = 'Sefki Kolozali'

from rabbitmq import RabbitMQ
import time


def hello_IoT_world():
    # establish connection
    host = "131.227.92.55"
    port = 8007
    rabbitmqconnection, rabbitmqchannel = RabbitMQ.establishConnection(host, port)

    # declare exchange
    exchange = 'Annotated_data'
    topic1 = 'Aarhus.Traffic.SensorID001'
    topic2 = 'Aarhus.Traffic.SensorID002'
    RabbitMQ.declareExchange(rabbitmqchannel, exchange, _type="topic")

    # send i messages
    i = 10
    for j in range(i):
        message = "sensory observation no: %i  --- hello IoT world!" % j
        RabbitMQ.sendMessage(message, rabbitmqchannel, exchange, topic1)
        message = "sensory observation no: %i  --- goodbye IoT world!" % j
        RabbitMQ.sendMessage(message, rabbitmqchannel, exchange, topic2)
        print("sent message %i" % j)
        time.sleep(10)

if __name__ == '__main__':
    hello_IoT_world()