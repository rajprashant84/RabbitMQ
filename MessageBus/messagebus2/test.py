__author__ = 'Sefki Kolozali'
from SPARQLWrapper import SPARQLWrapper, JSON
from pprint import pprint


queryString = "SELECT * WHERE { ?s ?p ?o. }"
sparql = SPARQLWrapper("http://iot.ee.surrey.ac.uk:8890/sparql")

# add a default graph, though that can also be done in the query string
#sparql.addDefaultGraph("http://iot.ee.surrey.ac.uk/citypulse/datasets/servicerepository")
#sparql.addDefaultGraph("http://iot.ee.surrey.ac.uk/citypulse/datasets/AarhusObservations")




queryString2='''
prefix g1: <http://iot.ee.surrey.ac.uk/citypulse/datasets/AarhusObservations>
prefix g2: <http://iot.ee.surrey.ac.uk/citypulse/datasets/servicerepository>
prefix ssn: <http://purl.oclc.org/NET/ssnx/ssn#>
prefix tl: <http://purl.org/NET/c4dm/timeline.owl#>
prefix sao: <http://purl.oclc.org/NET/UNIS/sao/sao#>
prefix ct: <http://ict-citypulse.eu/city#>

SELECT ?observation ?value ?observationTime ?property{
 {GRAPH g1: {
    ?observation a sao:Point .
    ?observation sao:value ?value .
    ?observation sao:time ?time .
    ?time tl:at ?observationTime .
    ?observation ssn:observedProperty ?property .
}} UNION {GRAPH g2: {
?property a ct:AverageSpeed .}}
}
    '''

queryString3='''prefix g1: <http://iot.ee.surrey.ac.uk/citypulse/datasets/AarhusObservations>
prefix g2: <http://iot.ee.surrey.ac.uk/citypulse/datasets/servicerepository>
            prefix ssn: <http://purl.oclc.org/NET/ssnx/ssn#>
            prefix tl: <http://purl.org/NET/c4dm/timeline.owl#>
            prefix sao: <http://purl.oclc.org/NET/UNIS/sao/sao#>
            prefix ct: <http://ict-citypulse.eu/city#>
            prefix owlssc: <http://www.daml.org/services/owl-s/1.2/ServiceCategory.owl#>
prefix ces: <http://www.insight-centre.org/ces#>

            SELECT ?propertyName ?EventService
            FROM NAMED g1:
            FROM NAMED g2:
            WHERE{
            GRAPH g1: {
               ?sensor ssn:observes <http://ict-citypulse.eu/property8e36634a-0728-43b3-8497-6b09071e2c82> .
               ?observation ssn:observedProperty <http://ict-citypulse.eu/property8e36634a-0728-43b3-8497-6b09071e2c82> . }
            GRAPH g2: {
               <http://ict-citypulse.eu/property8e36634a-0728-43b3-8497-6b09071e2c82> a ?propertyName .
               ?EventService ces:hasPhysicalEventSource ?sensor .
                 ?category owlssc:serviceCategoryName "'''+ "Aarhus_Road_Traffic"+ '''". }}'''

sparql.setQuery(queryString3)
sparql.setReturnFormat(JSON)
ret = sparql.query().convert() # ret is a stream with the results in XML, it is a file like object
for res in ret['results']['bindings']:
  #pprint(res['observation'])
    print(res)
   # pprint(res['value'])
    #pprint(res['observationTime'])
    # pprint(res['property'])
  # pprint(res['s'])
  # pprint(res['o'])
  # pprint(res['p'])


#
#   prefix g1: <http://example.com/g1#>
# prefix g2: <http://example.com/g2#>
# prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
#
# SELECT ?label (count(?s1) as ?g1count) (count(?s2) AS ?g2count)
# {
#   {
#     GRAPH g1: {
#       ?s1 rdfs:label ?label
#     }
#   } UNION {
#     GRAPH g2: {
#       ?s2 rdfs:label ?label
#     }
#   }
# } group by ?label order by ?label
