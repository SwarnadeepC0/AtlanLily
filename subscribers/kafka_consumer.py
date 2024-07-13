import json as json
import os
import sys
import threading

from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.structure.graph import Graph
from kafka import KafkaConsumer

from services.mongo_utils import MongoUtils


class GraphPublisher :
    daemon = True

    # Set up Kafka producer
    def run(self) :
        # Kafka Producer
        consumer = KafkaConsumer(
            bootstrap_servers=["localhost:29092"],
            value_deserializer=lambda m : json.loads(m.decode('utf-8'))
        )
        consumer.subscribe(
            ['deleteObject', 'insertObject', 'insertProperty', 'deleteProperty', 'insertRelationship'])
        #print('hi')
        while True :
            try :
                records = consumer.poll(10000, 500)
                for _, consumer_records in records.items():
                    for consumer_obj in consumer_records:
                        print(str(consumer_obj))
                        event = consumer_obj.value
                        print(str(consumer_obj.topic))
                        if consumer_obj.topic == 'deleteObject':
                            id = event['entity_id']
                            MongoUtils.deleteMany("objects",{'entity_id':id})
                        if consumer_obj.topic == 'insertObject' :
                            id = event['entity_id']
                            last_scan = event['scan_time']
                            classType = event['classType']
                            resourceName = event['name']
                            MongoUtils.insert("objects",{'entity_id':id,'last_scan':last_scan,
                                                             'classType':classType,'resourceName':resourceName})
                        if consumer_obj.topic == 'insertProperty' :
                            id = event['entity_id']
                            properties = event['properties']
                            for property in properties:
                                MongoUtils.update_one("objects", {'entity_id' : id}, {'$push' :
                                                                                          {'key' :property['key'], 'value':property['value']}})
                        if consumer_obj.topic == 'deleteProperty' :
                            id = event['entity_id']
                            properties = event['properties']
                            object = MongoUtils.find('objects', {'entity_id':id})
                            new_properties = []
                            for property in object[properties]:
                                if property['key'] not in properties:
                                    new_properties.append({'key':property['key'], 'value':property['value']})
                            object['properties']=new_properties
                            MongoUtils.insert('objects', object)
                        if consumer_obj.topic == 'insertRelationship' :
                            fromObj = event['from']
                            toObj = event['to']
                            relationship={}
                            relationship['from']=fromObj
                            relationship['to']=toObj
                            MongoUtils.insert('relationship',relationship)
                        print('yes')
            except Exception as e :
                print(e)
                sys.exit(1)
