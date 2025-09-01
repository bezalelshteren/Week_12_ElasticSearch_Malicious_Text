from app_ElasticSearch_Malicious_Text.crub_elastic import Crud_elastic
from app_ElasticSearch_Malicious_Text.data_loader import Dal
from app_ElasticSearch_Malicious_Text.delete_what_is_not_relevant import Delete_not_relevant
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
        self.deleter = Delete_not_relevant()


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
        return status

    def delete_not_relevant(self):
        list_documents = self.crud.search_by_query()
        print(list_documents)
        list_to_delete = self.deleter.check_if_delete_the_document(list_documents)
        status = self.crud.delete_by_search_or_id(list_to_delete)
        print(self.crud.es.count(index=self.crud.index_name))
        print(status)
        return status


m = Manager()
m.insert_from_the_csv()
m.update_new_fields()
m.delete_not_relevant()
# m.r()
#
#     def r(self):
#         e = self.crud.search_by_query({
#     "script": {
#         "script": {
#             "lang": "painless",
#             "source": """
#                 params._source.containsKey('weapons_list') &&
#                 params._source.weapons_list != null &&
#                 params._source.weapons_list instanceof List &&
#                 params._source.weapons_list.size() > 0
#             """
#         }
#     }
# }
# )
#
#         print(e)
