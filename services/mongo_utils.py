import os

try:
    import traceback
    from pymongo import MongoClient
    import logging
except ImportError as e:
    traceback.print_exc()
    print('Failed to import some of the modules.')
    raise RuntimeError('Failed to import some of the modules.')
except Exception as e:
    traceback.print_exc()
    print('Unknown exception while trying to import modules.')
    raise RuntimeError('Failed to import some of the modules.')

class MongoUtils(object):
    mongodb = None
    @staticmethod
    def get_mongodb():
        try:
            if MongoUtils.mongodb is not None:
                return MongoUtils.mongodb
            print("MongoDB is None, Connecting")
            host = os.getenv('MONGO_HOST')
            user_name = os.getenv('MONGO_USER')
            password = os.getenv('PASSWORD')
            db = os.getenv('DATABASE')
        except Exception as e:
            traceback.print_exc()
            raise RuntimeError('Error while reading configuration file.')

        try:
            host_base_url = 'mongodb://{user_name}:{password}@{ip}'.format(user_name=user_name,
                                                                                                password=password,
                                                                                                ip=host, db=db)
            print(host_base_url)
            MongoUtils.mongodb = MongoClient(host=host_base_url)[db]
            print('Connected to MongoDB...')
            return MongoUtils.mongodb
        except Exception as e:
            print('Failed to connect to MongoDB...')
            traceback.print_exc()
            raise RuntimeError('Failed to connect to MongoDB...')

    @staticmethod
    def insert(collection_name, insert_query):
        return MongoUtils.get_mongodb()[collection_name].insert_one(insert_query)

    @staticmethod
    def update_one(collection_name, search_query, update_query, upsert=False, bypass_document_validation=False):
        print(str(update_query))
        return MongoUtils.get_mongodb()[collection_name].update_one(search_query, update_query, upsert=upsert,
                                                   bypass_document_validation=bypass_document_validation)

    @staticmethod
    def update_many(collection_name, search_query, update_query, upsert=False, bypass_document_validation=False):
        return MongoUtils.get_mongodb()[collection_name].update_many(search_query, update_query, upsert=upsert,
                                                    bypass_document_validation=bypass_document_validation)

    @staticmethod
    def find_one(collection_name, search_query):
        return MongoUtils.get_mongodb()[collection_name].find_one(search_query)

    @staticmethod
    def aggregate(collection_name, pipeline):
        print(str(pipeline))
        return MongoUtils.get_mongodb()[collection_name].aggregate(pipeline)

    @staticmethod
    def rename_collection(collection_name, targetName):
        print('Running rename_collection on collection {}'.format(collection_name))
        print('target collection name'.format(targetName))
        return MongoUtils.get_mongodb()[collection_name].rename(targetName)

    @staticmethod
    def list_collection_names():
        return MongoUtils.get_mongodb().list_collection_names()

    @staticmethod
    def find(collection_name, search_query) :
        return MongoUtils.get_mongodb()[collection_name].find(search_query)

    @staticmethod
    def deleteMany(collection_name, delete_query):
        return MongoUtils.get_mongodb()[collection_name].delete_many(delete_query)
