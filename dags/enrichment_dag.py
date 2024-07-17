# from datetime import datetime
# import os
# import time
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from model_service import ModelService
#
#
# def get_children(path, name, scan_time,resource_name, add_properties, delete_properties) :
#     from mongo_utils import MongoUtils
#     modelService = ModelService()
#     if os.path.isfile(path):
#         classType = 'file'
#     else:
#         classType = 'folder'
#     object_doc = {'entity_id' : path, 'name' : path, 'classType' : classType, 'name' : name, 'last_scan' : scan_time,
#                        'resource_name': 'enrichment', 'properties' : [add_properties]}
#     MongoUtils.insert('stage_objects', object_doc)
#     if os.path.isfile(path):
#         return;
#     for file in os.listdir(path):
#         get_children(path+"/"+file, path, scan_time, resource_name)
#
# def ScanAndStageTask(**kwargs):
#     from mongo_utils import MongoUtils
#     print('Runing scan and stage')
#     try:
#         #enrichment_id = kwargs['dag_run'].conf["run_id"]
#         # resource_run = mongodb.find_one("resource_run", {"_id":ObjectId(run_id)})
#         # resource = resource_run['conf']
#         from bson import ObjectId
#         enrichment_id = ""
#         enrichment_request = MongoUtils.find_one("enrichmentRequest", {"enrichment_id":ObjectId(enrichment_id)})
#         name = "test"
#         scan_time = round(time.time() * 1000)
#         modelService = ModelService()
#         properties = modelService.get_all_attributes(class_type='resource',model_name='resource', custom=False)
#         entity_id = enrichment_request['entity_id']
#         add_properties = enrichment_request['add_properties']
#         delete_properties = enrichment_request['delete_properties']
#
#         object = MongoUtils.find_one('stage_objects', {'entity_id':entity_id})
#
#         get_children(object['name'], name, scan_time, name, add_properties, delete_properties)
#     except Exception as e:
#         print(e)
#
#
# def insert_event_for_objects(eventProducer, object) :
#     insert_event = {
#         'entity_id':object['entity_id'],
#         'classType': object['classType'],
#         'scan_time':object['last_scan'],
#         'name': object['name']
#     }
#     eventProducer.send(insert_event, "insertObject")
#
#
# def insert_event_for_attributes(eventProducer, properties, entity_id) :
#     property_change=[]
#     for property in properties:
#         property_change.append({
#            "key":property['id'],
#             "value":property['value']
#         });
#     insert_property = {
#         "entity_id":entity_id,
#         'properties': property_change
#     }
#     eventProducer.send(insert_property, "insertProperty")
#
#
# def insert_event_for_relationships(eventProducer, relationship) :
#     insert_relationship = {
#         'from' : relationship['from'],
#         'to' : relationship['to'],
#         'scan_time' : relationship['last_scan'],
#         'resource_name' : relationship['resource_name']
#     }
#     eventProducer.send(insert_relationship, "insertRelationship")
#
#
# def delete_event_for_objects(eventProducer, object) :
#     delete_obj = {
#         'entity_id':object['entity_id']
#     }
#     eventProducer.send(delete_obj, "deleteObject")
#
#
# def delete_event_for_property(event_producer, key, entity_id) :
#     property_change = []
#     property_change.append({
#         'key':key
#     })
#     delete_property = {
#         "entity_id" : entity_id,
#         'properties' : property_change
#     }
#     event_producer.send(delete_property,'deleteProperty')
#
# def insert_event_for_property(event_producer, key, value, entity_id) :
#     property_change = []
#     property_change.append({
#         'key':key,
#         'value':value
#     })
#     insert_propperty = {
#         "entity_id" : entity_id,
#         'properties' : property_change
#     }
#     event_producer.send(insert_propperty,'insertProperty')
#
#
# def ResolveTask(**kwargs):
#     from event_producer import EventProducer
#     from mongo_utils import MongoUtils
#     #run_id = kwargs['dag_run'].conf["run_id"]
#     #mongodb = MongoUtils.get_mongodb()
#     #resource_run = mongodb.find_one("resource_run", {"_id" : ObjectId(run_id)})
#     #resource = resource_run['conf']
#     cursor = MongoUtils.aggregate("stage_objects", [
#     {
#         '$match': {
#             'resource_name': 'enrichment'
#         }
#     }, {
#         '$sort': {
#             'last_scan': -1
#         }
#     }, {
#         '$limit': 2
#     }, {
#         '$project': {
#             'last_scan': 1
#         }
#     }
# ])
#     eventProducer = EventProducer();
#     scan_times = []
#     for scan_time in cursor:
#         scan_times.append(scan_time['last_scan'])
#     print(scan_times)
#     if len(scan_times)==1:
#         objects = MongoUtils.find('stage_objects', {'resource_name':name, 'last_scan':{'$gte':scan_times[0]}})
#         objectCol = []
#         for object in objects:
#             insert_event_for_objects(eventProducer, object)
#             objectCol.append(object)
#         for object in objectCol:
#             insert_event_for_attributes(eventProducer, object['properties'], object['entity_id'])
#         relationships = MongoUtils.find('stage_relationship', {'resource_name':'test','last_scan':{'$gte':scan_times[0]}})
#         for relationship in relationships:
#             insert_event_for_relationships(eventProducer, relationship)
#     if len(scan_times)>2:
#         objects = MongoUtils.find('stage_objects', {'resource_name' : name, 'last_scan' : {'$gte' : scan_times[0]}})
#         prev_objects = MongoUtils.find('stage_objects', {'resource_name' : name, 'last_scan' : {'$gte' : scan_times[1], "$lte": scan_times[0]}})
#         curr_obj_dict={}
#         prev_obj_dict = {}
#         deleteObject = []
#         insertObjects = []
#         insertProperty = []
#         deleteProperty = []
#         curr_obj_property = {}
#         prev_obj_property = {}
#         for object in objects:
#             curr_obj_dict[object['entity_id']]=object
#         for object in prev_objects:
#             identity = object['entity_id']
#             if identity not in curr_obj_dict:
#                 deleteObject.append(object)
#             else :
#                 curr_obj = curr_obj_dict[identity]
#                 for property in curr_obj['properties']:
#                     curr_obj_property[property['key']+":"+identity] = {'id':identity, 'value':property['value']}
#                 for property in prev_obj_dict['properties']:
#                     prev_obj_property[property['key']+":"+identity] = {'id':identity, 'value':property['value']}
#                 prev_obj_dict[object['entity_id']]=object
#         for key,value in curr_obj_dict:
#             if key not in prev_obj_dict:
#                 insertObjects.append({'key':key, 'value':value})
#         for key, value in curr_obj_property:
#             if key not in prev_obj_property:
#                 insertProperty.append({'key':key, 'value':value})
#             else :
#                 if value != prev_obj_property[key]:
#                     insertProperty.append({'key':key, 'value':value})
#         for key, value in prev_obj_property:
#             if key not in curr_obj_property :
#                 deleteProperty.append({'key':key, 'value':value})
#         for object in insertObjects:
#             insert_event_for_objects(eventProducer, object)
#         for object in  deleteObject:
#             delete_event_for_objects(eventProducer, object)
#         for property in  deleteProperty:
#             delete_event_for_property(deleteProperty['key'].split(":")[0],deleteProperty['key'].split(":")[1])
#         MongoUtils.deleteMany('stage_objects', {'resource_name' : 'enrichment', 'last_scan' : {'$gte' : scan_times[0]}})
#     eventProducer.close()
#
#
#
#
#
#
#
# with DAG("enrichment_dag",
#          start_date= datetime.min,
#          catchup=False,
#          tags=["atlan"]) as dag :
#     scanAndStage = PythonOperator(
#         task_id="scan_task",
#         python_callable = ScanAndStageTask,
#         provide_context=True,
#         dag=dag
#     )
#     resolve = PythonOperator(
#         task_id="resolve_task",
#         python_callable=ResolveTask,
#         provide_context=True,
#         dag=dag
#     )
#
#     scanAndStage >> resolve
