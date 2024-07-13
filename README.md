
# Atlan Lily


## Problem statement
1.) (INBOUND, EXTERNAL) : A customer uses Monte Carlo as a tool for data observability. They have set it up so that Monte Carlo catches any table health or data reliability issues early on. The customer would like Atlan to also become a near-real-time repository of such issues, with relevant metadata attached to respective assets.

2.)(INBOUND, INTERNAL) : A prospect has a metadata estate spanning 1B metadata assets. While the bulk of this payload is columns in different tables and BI fields (~90% of total), the remaining 10% consists of assets such as databases, schemas, tables, and dashboards. They want to ingest metadata using Atlan’s metadata extraction with an 80-20 rule, where columns become eventually consistent in the metadata lake.

3.) (OUTBOUND, INTERNAL) : There are internal enrichment automation requirements towards metadata into Atlan, such that any change in the Atlan entity triggers similar changes to entities connected downstream in lineage from that entity.

4.) (OUTBOUND, EXTERNAL) : A customer of Atlan wants to enforce data access security and compliance. They require that as soon as an entity is annotated as PII or GDPR in Atlan, their downstream data tools become aware of it and enforce access control while running SQL queries on the data.

## Functional Requirement:

* (INBOUND, EXTERNAL)
    * Atlan should store data send from Monte carlo corresponding to the assets
* (INBOUND, INTERNAL)
    * Data should be eventually consistent
    * Enrichments made by the customer should not be lost due to rescanning
    * System should be able to detect changes in the entities from the last scan
    * Need to support various types of assets
*   (OUTBOUND, INTERNAL): 
    * User should be able to add/delete/update enrichments i.e. additional information to the object. 
    * Supporting enrichment for single key:value for now
    * Enrichments changes on the parent should be migrated to child
*   (OUTBOUND, EXTERNAL)
    * Data access flag i.e. PII or GDPR should be added as part of notification to the consumers
    * Data access flags should be added to downstream entities as well



## Non Functional Requirement:
- (INBOUND, EXTERNAL)
    * Near real time 
* (INBOUND, INTERNAL)
    * Eventual consistency
* (OUTBOUND, INTERNAL): 
    * Enrichment should not lost after rescan


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
![Alt text](designs/ingestion.jpg?raw=true "Title")
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
![Alt text](designs/ingestion_user_flow.jpg?raw=true "Title")
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

  ![Alt text](designs/enrichment_service.jpg?raw=true "Title")

* To enrich any entity atlan user has to fire enrichment request providing the entity_id with attribute values to be updated

* The enrichment request gets submitted to kafka queue

* The Enrichment service listens to this enrichment request

* Updates the user facing graph store , if property is to be propagated enrichment service hirarchially traverse the objects and it children and update the property placing the update property event back to kafka queue

![Alt text](designs/enrichment_user_flow.jpg?raw=true "Title")

(INBOUND, External) & (OUTBOUND, EXTERNAL):
![Alt text](designs/issue_generator.jpg?raw=true "Title")
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







