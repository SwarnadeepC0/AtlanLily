# This is a sample Python script.
import sys
import time

import resource_service
from subscribers.enrichment_service import EnrichmentService
from subscribers.kafka_consumer import GraphPublisher

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.




# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    command_type = sys.argv[1]
    if(command_type == "create_resource"):
        resource_name = sys.argv[2]
        resource_type = sys.argv[3]
        if resource_type != 'file_system':
            raise Exception('Only file system resource is supported')
        abs_path = sys.argv[4]
        resource_service = resource_service.ResourceService()
        resource_service.save(resource_name, abs_path)
    if command_type == "run_resource":
        resource_name = sys.argv[2]
        resource_service = resource_service.ResourceService()
        resource_service.run(resource_name)
        publisher = GraphPublisher()
        publisher.run()
    if command_type == "enrichment":
        publisher = EnrichmentService()
        publisher.start()
        entity_id = sys.argv[2]
        resource_service = resource_service.ResourceService()
        key = sys.argv[3]
        value = sys.argv[4]
        time.sleep(3)
        resource_service.enrich(entity_id,key,value)
        publisher.join()




# See PyCharm help at https://www.jetbrains.com/help/pycharm/
