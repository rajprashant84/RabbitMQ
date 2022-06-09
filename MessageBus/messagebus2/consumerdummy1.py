import rdflib

__author__ = 'Sefki Kolozali'
from rabbitmq import RabbitMQ
class consumerDummy(object):
    """
    Default listener, to test if messages are sent through the message bus.
    """
    def __init__(self, exchange, key):
        self.host = "131.227.92.55"
        self.port = 8007
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


        query2='''prefix ssn: <http://purl.oclc.org/NET/ssnx/ssn#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
prefix tl: <http://purl.org/NET/c4dm/timeline.owl#>
prefix sao: <http://purl.oclc.org/NET/UNIS/sao/sao#>
prefix ct: <http://ict-citypulse.eu/city#>
prefix owlssc: <http://www.daml.org/services/owl-s/1.2/ServiceCategory.owl#>
Select ?observation (COUNT(?observation) as ?obscount) (COUNT(?sensorid) as ?senscount)

{?observation a sao:Point .
?observation sao:value ?observationvalue .
?observation ssn:observedBy ?sensorid .}
'''
        print(body)
        graph = rdflib.ConjunctiveGraph()
        graph.parse(data=body, format="n3")
        list=[]
        for res in graph.query(query2).bindings:
                    # print 'test'
                    # sensorsax= res["sensorsax"]
            print("point ID: ", res["obscount"].toPython())
            print("sensor ID: ", res["senscount"].toPython())

            pointid=res['observation'].toPython()
            list.append(pointid)
        print(list)
        print(len(list))


if __name__ == '__main__':
    #Listens to exchange "hello" on topic "world"
    # consumer = consumerDummy('Social_data', 'Aarhus.Twitter')
    consumer = consumerDummy('annotated_data', 'Aarhus.Road.Traffic.#')

    #Listens to exchange "hello" on topic "world"
    #consumer = consumerDummy('Annotated_data', 'Aarhus.Traffic.SensorID002')
    consumer.start()