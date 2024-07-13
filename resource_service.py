import json
import time

from bson import ObjectId

from services.mongo_utils import MongoUtils

class ResourceService:

    def save(self, resource_name, abs_path) :
        print("Save")
        return MongoUtils.insert("resource", {"resource_name" : resource_name, "absolute_path" : abs_path})

    def run(self, resource_name) :
        mongodb = MongoUtils.get_mongodb()
        resource = mongodb.find_one("resource", {"resource_name" : resource_name})
        run_id = ObjectId()
        MongoUtils.insert("resource_run",
                       {"conf" : resource, "resource_run" : run_id, "run_at" : round(time.time() * 1000)})
        from airflow.api.common.experimental import trigger_dag
        conf = json.dumps({
            "workflow" : "ingestion_dag",
            "run_id" : str(run_id),
        })
        trigger_dag.trigger_dag("ingestion_dag", str(run_id), conf=conf)

    def enrich(self, entity_id, key, value) :
        mongodb = MongoUtils.get_mongodb()
        enrichment_id = ObjectId()
        MongoUtils.insert("enrichment_request",
                          {"enrichment_id":enrichment_id,"entity_id":entity_id, "add_properties":[{"key":key, "value": value}]})
        from airflow.api.common.experimental import trigger_dag
        conf = json.dumps({
            "workflow" : "enrichment_dag",
            "enrichment_id" : str(enrichment_id),
        })
        trigger_dag.trigger_dag("enrichment_dag", str(enrichment_id), conf=conf)

