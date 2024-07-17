import json as json
import sys
import threading

from bson import ObjectId
from kafka import KafkaConsumer

from services.mongo_utils import MongoUtils


class EnrichmentService(threading.Thread) :
    daemon = True

    # Set up Kafka producer
    def run(self) :
        # Kafka Producer
        print("Starting enrichment service")
        consumer = KafkaConsumer(
            'enrichRequest',
            bootstrap_servers=["localhost:29092"],
            value_deserializer=lambda m : json.loads(m.decode('utf-8')))
        for message in consumer :
            event = message.value
            if message.topic == 'enrichRequest' :
                enrichment_id = event['enrichment_id']
                if id == 'poison_kill' :
                    print('Enrichment done')
                    sys.exit(0)
                # print(str(enrichment_id))
                enrichment_requests = MongoUtils.find_one('enrichment_request',
                                                          {'enrichment_id' : ObjectId(enrichment_id)})

                self.update_downstream_child(enrichment_requests)

    def update_downstream_child(self, enrichment_requests) :
        cursor = MongoUtils.aggregate("relationship", [
            {
                '$match' : {
                    'from' : enrichment_requests['entity_id']
                }
            }, {
                '$graphLookup' : {
                    'from' : 'relationship',
                    'startWith' : '$from',
                    'connectFromField' : 'to',
                    'connectToField' : 'from',
                    'as' : 'reportingHierarchy'
                }
            }, {
                '$project' : {
                    'from' : 1,
                    'tos' : {
                        '$map' : {
                            'input' : '$reportingHierarchy',
                            'as' : 'tos',
                            'in' : {
                                'name' : '$$tos.to'
                            }
                        }
                    }
                }
            }, {
                '$unwind' : {
                    'path' : '$tos'
                }
            }, {
                '$group' : {
                    '_id' : '$from',
                    'childs' : {
                        '$push' : '$tos.name'
                    }
                }
            }
        ])
        properties = {}
        parent_child_collection = None
        try :
            parent_child_collection = cursor.next()
        except StopIteration :
            print("Empty cursor!")
        for property in enrichment_requests['add_properties'] :
            properties[property['key']] = property['value']
        object = MongoUtils.insert('objects', {'entity_id' : enrichment_requests['entity_id']})
        for property in object['properties'] :
            if property['key'] in properties :
                property['key'] = properties[property['key']]
            else :
                property['key'] = property['value']
        MongoUtils.update_one('objects', {'entity_id' : enrichment_requests['entity_id']},
                              {'$push' : {'properties' : properties}})
        parsed_childs = []
        if parent_child_collection is not None :
            for child in parent_child_collection['childs'] :
                if child != enrichment_requests['entity_id'] and child not in parsed_childs :
                    MongoUtils.update_one('objects', {'entity_id' : child}, {'$push' : {'properties' : properties}})
                    for property in object['properties'] :
                        if property['key'] in properties :
                            property['key'] = properties[property['key']]
                        else :
                            property['key'] = property['value']
                    parsed_childs.append(child)

