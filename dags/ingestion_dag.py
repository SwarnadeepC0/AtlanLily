from datetime import datetime
import os
import time
from airflow import DAG
from airflow.operators.python import PythonOperator
from model_service import ModelService


def get_children(path, name, scan_time,resource_name) :
    from mongo_utils import MongoUtils
    modelService = ModelService()
    if os.path.isfile(path):
        class_type = 'file'
    else:
        class_type = 'folder'
    properties = modelService.get_all_attributes(class_type=class_type, model_name='filesystem', custom=False)
    object_doc = {'entity_id' : path, 'name' : path, 'classType' : class_type, 'last_scan' : scan_time,
                       'resource_name': resource_name, 'properties' : []}
    for property in properties:
        if property['id'] == 'core.name' :
            value = name
        if property['id'] == 'filesystem.modifiedAt':
            value = round(os.path.getmtime(path)*1000)
        object_doc['properties'].append({'id' : property['id'], 'value' : value})
    MongoUtils.insert('stage_objects', object_doc)
    relationship_doc = {'from':name, 'to':path, 'resource_name': resource_name, 'scan_time':scan_time}
    MongoUtils.insert('stage_relationship', relationship_doc)
    if os.path.isfile(path):
        return
    for file in os.listdir(path):
        get_children(path+"/"+file, path, scan_time, resource_name)

def ScanAndStageTask(**kwargs):
    from mongo_utils import MongoUtils
    from bson import ObjectId
    print('Runing scan and stage')
    try:
        run_id = kwargs['dag_run'].conf["run_id"]
        resource_run = MongoUtils.find_one("resource_run", {"_id":ObjectId(run_id)})
        resource = resource_run['conf']
        from bson import ObjectId
        resource = MongoUtils.find_one("resource", {"resource_name":resource['resource_name']})
        name = resource['resource_name']
        scan_time = round(time.time() * 1000)
        modelService = ModelService()
        properties = modelService.get_all_attributes(class_type='resource',model_name='resource', custom=False)
        resource_doc = {'entity_id' : name, 'name' : name, 'classType' : 'resource', 'name' : name,
                        'last_scan' : scan_time, 'properties' : []}
        for property in properties :
            if property['id'] == 'core.name' :
                value = name
            resource_doc['properties'].append({'id' : property['id'], 'value' : value})
        #print(str(resource))
        MongoUtils.insert('stage_objects', resource_doc)
        get_children(resource['absolute_path'], name, scan_time, name)
    except Exception as e:
        print(e)


def insert_event_for_objects(eventProducer, object) :
    insert_event = {
        'entity_id':object['entity_id'],
        'classType': object['classType'],
        'scan_time':object['last_scan'],
        'name': object['name']
    }
    eventProducer.send(insert_event, "insertObject")


def insert_event_for_attributes(eventProducer, properties, entity_id) :
    property_change=[]
    for property in properties:
        property_change.append({
           "key":property['id'],
            "value":property['value']
        });
    insert_property = {
        "entity_id":entity_id,
        'properties': property_change
    }
    eventProducer.send(insert_property, "insertProperty")


def insert_event_for_relationships(eventProducer, relationship) :
    insert_relationship = {
        'from' : relationship['from'],
        'to' : relationship['to'],
        'scan_time' : relationship['scan_time'],
        'resource_name' : relationship['resource_name']
    }
    eventProducer.send(insert_relationship, "insertRelationship")


def delete_event_for_objects(eventProducer, object) :
    delete_obj = {
        'entity_id':object['entity_id']
    }
    eventProducer.send(delete_obj, "deleteObject")


def delete_event_for_property(event_producer, key, entity_id) :
    property_change = []
    property_change.append({
        'key':key
    })
    delete_property = {
        "entity_id" : entity_id,
        'properties' : property_change
    }
    event_producer.send(delete_property,'deleteProperty')

def insert_event_for_property(event_producer, key, value, entity_id) :
    property_change = []
    property_change.append({
        'key':key,
        'value':value
    })
    insert_propperty = {
        "entity_id" : entity_id,
        'properties' : property_change
    }
    event_producer.send(insert_propperty,'insertProperty')


def ResolveTask(**kwargs):
    from event_producer import EventProducer
    from mongo_utils import MongoUtils
    run_id = kwargs['dag_run'].conf["run_id"]
    from bson import ObjectId
    resource_run = MongoUtils.find_one("resource_run", {"_id" : ObjectId(run_id)})
    resource = resource_run['conf']
    name = resource['resource_name']
    cursor = MongoUtils.aggregate("stage_objects", [
    {
        '$match': {
            'entity_id': name
        }
    }, {
        '$sort': {
            'last_scan': -1
        }
    }, {
        '$limit': 2
    }, {
        '$project': {
            'last_scan': 1
        }
    }
])
    eventProducer = EventProducer()
    scan_times = []
    for scan_time in cursor:
        scan_times.append(scan_time['last_scan'])
    print('scan_time'+str(len(scan_times)))
    if len(scan_times)==1:
        objects = MongoUtils.find('stage_objects', {'resource_name':name, 'last_scan':{'$gte':scan_times[0]}})
        object_col = []
        for object in objects:
            insert_event_for_objects(eventProducer, object)
            object_col.append(object)
        for object in object_col:
            insert_event_for_attributes(eventProducer, object['properties'], object['entity_id'])
        relationships = MongoUtils.find('stage_relationship', {'resource_name':name,'scan_time':{'$gte':scan_times[0]}})
        for relationship in relationships:
            insert_event_for_relationships(eventProducer, relationship)
    elif len(scan_times)>=2:
        objects = MongoUtils.find('stage_objects', {'resource_name' : name, 'last_scan' : {'$gte' : scan_times[0]}})
        prev_objects = MongoUtils.find('stage_objects', {'resource_name' : name, 'last_scan' : {'$gte' : scan_times[1], "$lt": scan_times[0]}})
        curr_obj_dict={}
        prev_obj_dict = {}
        deleteObject = []
        insertObjects = []
        insertProperty = []
        deleteProperty = []
        curr_obj_property = {}
        prev_obj_property = {}
        for object in objects:
            curr_obj_dict[object['entity_id']]=object
        for object in prev_objects:
            identity = object['entity_id']
            if identity not in curr_obj_dict:
                print("Deleted object")
                deleteObject.append(object)
            else :
                curr_obj = curr_obj_dict[identity]
                for property in curr_obj.get('properties',[]):
                    curr_obj_property[property['id']+":"+identity] = {'id':identity, 'value':property['value']}
                for property in prev_obj_dict.get('properties',[]):
                    prev_obj_property[property['id']+":"+identity] = {'id':identity, 'value':property['value']}
                prev_obj_dict[object['entity_id']]=object
        for key,value in curr_obj_dict.items():
            if key not in prev_obj_dict:
                insertObjects.append(value)
        for key, value in curr_obj_property.items():
            if key not in prev_obj_property:
                insertProperty.append({'key':key, 'value':value})
            else :
                if value != prev_obj_property[key]:
                    insertProperty.append({'key':key, 'value':value})
        for key, value in prev_obj_property.items():
            if key not in curr_obj_property :
                deleteProperty.append({'key':key, 'value':value})
        for object in insertObjects:
            insert_event_for_objects(eventProducer, object)
        for object in  deleteObject:
            delete_event_for_objects(eventProducer, object)
        for property in  deleteProperty:
            delete_event_for_property(deleteProperty['key'].split(":")[0],deleteProperty['key'].split(":")[1])
    eventProducer.send({'entity_id':'poison_kill'},'deleteObject')

    eventProducer.close()







with DAG("ingestion_dag",
         start_date= datetime.min,
         catchup=False,
         tags=["atlan"]) as dag :
    scanAndStage = PythonOperator(
        task_id="scan_task",
        python_callable = ScanAndStageTask,
        provide_context=True,
        dag=dag
    )
    resolve = PythonOperator(
        task_id="resolve_task",
        python_callable=ResolveTask,
        provide_context=True,
        dag=dag
    )

    scanAndStage >> resolve
