from pymongo import MongoClient
import jproperties
import logging


log = logging.getLogger(__name__)

logging.basicConfig(filename='cars_price.log', level=logging.INFO)



class PyMongo:


    def __init__(self):

        self.client = MongoClient()

        return None
    
    def connect_db(self, name):

        self.db = eval(f'self.client.{name}')
        log.info('Connection Sucess')

        return self.db
    
    def insert_many(self, name, collection_name, data:list):

        self.db = self.connect_db(name)
        eval(f'self.db.{collection_name}.insert_many(data)')

        log.info('insert many -- sucess')
        
        return None

    def insert_one(self, name, collection_name, data: dict):
        self.db = self.connect_db(name)
        eval(f'self.db.{collection_name}.insert_one(data)')

        log.info('insert one -- sucess')

        return None