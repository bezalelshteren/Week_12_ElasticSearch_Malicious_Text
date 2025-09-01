from app_ElasticSearch_Malicious_Text.crub_elastic import Crud_elastic
from app_ElasticSearch_Malicious_Text.data_loader import Dal
from dotenv import load_dotenv
from app_ElasticSearch_Malicious_Text.processor import Processor
import os

load_dotenv()


elasticsearch_url = os.getenv("ELASTIC_URL")
indices_name = os.getenv("INDEX_NAME")
path_csv = os.getenv("CSV_PATH")
weapons_path = os.getenv("WEAPONS_LIST_PATH")


class Manager:
    def __init__(self):
        self.dal = Dal()
        self.weapons = self.dal.read_list_weapons(weapons_path)
        self.crud = Crud_elastic(elasticsearch_url)
        self.processor = Processor(self.weapons)

    def insert_from_the_csv(self):
        list_doc = self.dal.read_from_file_to_json(path_csv)
        # self.crud.create_index()
        num_inserted = self.crud.insert_all(list_doc)
        return f"{num_inserted} is inserted"

    def update_new_fields(self):
        list_documents = self.crud.search_by_query()
        print(self.crud.es.count(index=self.crud.index_name))
        list_to_update = self.processor.start_all_process_to_update(list_documents)
        status = self.crud.update_document(list_to_update)
        print(status)
        # print(self.crud.search_by_query())
        return status


m = Manager()
m.insert_from_the_csv()
m.update_new_fields()