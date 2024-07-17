import json
import time

from bson import ObjectId

from services.event_producer import EventProducer
from services.mongo_utils import MongoUtils


class ResourceService :

    def save(self, resource_name, abs_path) :
        return MongoUtils.insert("resource", {"resource_name" : resource_name, "absolute_path" : abs_path})

    def run(self, resource_name) :
        resource = MongoUtils.find_one("resource", {"resource_name" : resource_name})
        run_id = ObjectId()
        MongoUtils.insert("resource_run",
                          {"conf" : resource, "_id" : run_id, "run_at" : round(time.time() * 1000)})
        from airflow.api.common.experimental import trigger_dag
        conf = json.dumps({
            "workflow" : "ingestion_dag",
            "run_id" : str(run_id),
        })
        trigger_dag.trigger_dag("ingestion_dag", str(run_id), conf=conf)

    def enrich(self, entity_id, key, value) :
        enrichment_id = ObjectId()
        enrichment_requests = {"enrichment_id" : enrichment_id, "entity_id" : entity_id,
                               "add_properties" : [{"key" : key, "value" : value}]}
        MongoUtils.insert("enrichment_request",
                          enrichment_requests)

        event_producer = EventProducer()
        event_producer.send( {'enrichment_id':str(enrichment_id)},"enrichRequest")
        event_producer.send({'entity_id':'poison_kill'},'deleteObject')
        event_producer.close()

