
## APIs:
* createResource(resource_conf, resource_name)
* ingest(resource_name)
* getEntity(entity_id, children=True)
* enrichEntity(entity_id, enrichment_request)
* subscribe(entity_id, details)
* notify(entity_id, subscriber_id, event)
* schedule(entity, query)

## Terminology
* Resources : Datasets / Sources that the customer wants to perform metadata extraction on
* Subscribers: Tools and services subscribed to change events on a entity

## Design 
(INBOUND, INTERNAL) & (INBOUND, INTERNAL)
* Model Service : This service will keep blueprint for the entity and relationship
```json
{
    id: <model_name>,
    entityType:{
        {   
            id : <model_name>.Table
            type:Table
        },{   
            id : <model_name>.Column
            type:Column
        }
    },
    properties:{
        {
            id: <property_id>,
            name: <property_name>,
            classType: Column,
            propagate: true/false
        }
    }
}
```
* User scan a resource and queries model services for all the attributes that can be fetched from the perticular resource type

* It then creates a document with entity and relationship like so

```json
{   
    id: <entity_id>
    name: ##<resource_name>##<database_name>
    type: entity
    objectType: database
    resource_name:<resource_name>
    resource_type:<resource_type>
    scan_date: <date>
    properties: [
        {
            id:core.name
            value: <entity_name>
            propagate: false
        }
    ]
}
{
    from: ##<resource_name>##<database_name>
    to: ##<resource_name>##<database_name>##<schema_name>
    type: relationship
    scan_date: <date>
    relationType: parentChild
    resource_name:<resource_name>
    resource_type:<resource_type>
}
```

* The staging task can then insert this document to iceberg table along with the timestamp of the scan , this will act as our staging db 
* The iceberg data can be inserted in parquet format, will have database created based on
    *   <resource_type>.objects partitioned on resource name and timestamp
    *   <resource_type>.edges partitioned on resource name and timestamp
    *   <type>.properties partitioned on resource, timestamp, 
* The data in the iceberg table then resolved and difference is found between the two scan using timestamp.
Find the deleted object, delete edge corresponding to them, deleted properties and create change events for all of them

* The change events is pushed to a kafka queue, with will have consumer can be graph publisher , which will recieve the change event for insertion and deletion and create a graph out of it

* To enrich any entity atlan user has to fire enrichment request providing the entity_id with attribute values to be updated

* The enrichment request gets submitted to kafka queue

* The Enrichment service listens to this enrichment request

* Updates the user facing graph store , if property is to be propagated enrichment service hirarchially traverse the objects and it children and update the property placing the update property event back to kafka queue

(INBOUND, External) & (OUTBOUND, EXTERNAL):
* Monte Carlo can be configured with webhooks calling the atlan apis to add issues for the entities

* The issues can be added as a custom attribute for any entity that customer is monitoring for entities

* The enrichment request for entities is sent as a event to kafka queue. 

* Enrichment Service is consumer for the kafka queue.

* It applies the enrichments on top of the frontend facing graph database. 

* And submits notification event

* The downstream consumers get consumes the event and become aware of the entities issues

* PII or GDPR is set as a property on entity. 

* This enrichment request is sent as event to kafka queue, to which enrichment service is subscribed to 

* The enrichment service applies the enrichment on frontend, propagate it if required and place the property update event back to kafka queue

* The downstream component will become aware of all the entities that will have PII or GDPR value set and store them at their end for future reference. 








