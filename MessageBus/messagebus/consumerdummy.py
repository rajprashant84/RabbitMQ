from rabbitmq import RabbitMQ
__author__ = 'Sefki Kolozali'


class consumerDummy(object):
    """
    Default listener, to test if messages are sent through the message bus.
    """

    def __init__(self, exchange, key):
        self.host = "localhost"
        self.port = 15672

        self.connection, self.channel = RabbitMQ.establishConnection(self.host, self.port)
        self.exchange = exchange
        self.routing_key = key

    def start(self):
        RabbitMQ.declareExchange(self.channel, self.exchange, _type="topic")
        queue = self.channel.queue_declare()
        queue_name = queue.method.queue
        self.channel.queue_bind(exchange=self.exchange, queue=queue_name, routing_key=self.routing_key)
        self.channel.basic_consume(self.onMessage,  no_ack=True)
        self.channel.start_consuming()

    def stop(self):
        self.channel.stop_consuming()

    def onMessage(self, ch, method, properties, body):
        # print ch, method, properties
        print(body)

if __name__ == '__main__':
    # Listens to everything published on annotated data exchange
    consumer = consumerDummy('annotated_data', '#')

    # Listens to exchange "hello" on topic "world.#" (both) wild card "#" helps to receive everything
    consumer = consumerDummy('Annotated_data', 'Aarhus.Traffic.#')

    #Listens to exchange "hello" on topic "world"
    #consumer = consumerDummy('Annotated_data', 'Aarhus.Traffic.3')

    #Listens to exchange "hello" on topic "world"
    #consumer = consumerDummy('Annotated_data', 'Aarhus.Traffic.3')

    consumer.start()