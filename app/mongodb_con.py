from pymongo import MongoClient
import jproperties
import logging


log = logging.getLogger(__name__)

logging.basicConfig(filename='cars_price.log', level=logging.INFO)



class PyMongo:


    def __init__(self):

        self.client = MongoClient()

        return None
    
    def _connect_db(self, db_name):

        self.db = eval(f'self.client.{db_name}')
        log.info('Connection Sucess')

        return self.db
    
    def _current_data(self, db_name, collection_name):

        self.db = self._connect_db(db_name)
        self.current_data = list(
                                    eval(f'self.db.{collection_name}').find({})
                                    )
        
        return self.current_data
    
    
    def insert_many(self, db_name, collection_name, data:list):

        # first check if the id exists on DB;
        self.current_data = self._current_data(db_name, collection_name)
        self.existing_ids = []
        self.db_name = self._connect_db(db_name)
        
        for obj in self.current_data:
            self.id = obj.get('_id')
            self.existing_ids.append(self.id)

        # compare;
        to_update = []
        for doc in data:
            if doc.get('_id') in self.existing_ids:
                to_update.append(doc)
            else:
                eval(f'self.db_name.{collection_name}.insert_one(doc)')


        if len(to_update) > 0:
            # them update;

        log.info('insert many -- sucess')
        
        return None
    

    # def insert_one(self, db_name, collection_name, data: dict):
    #     self.db = self.connect_db(db_name)
    #     eval(f'self.db.{collection_name}.insert_one(data)')

    #     log.info('insert one -- sucess')

    #     return None