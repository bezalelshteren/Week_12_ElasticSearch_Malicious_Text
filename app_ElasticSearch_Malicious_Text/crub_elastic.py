import os
from dotenv import load_dotenv
from elasticsearch import Elasticsearch,helpers
from elasticsearch.helpers import scan
load_dotenv()




elasticsearch_url = os.getenv("ELASTIC_URL")
indices_name = os.getenv("INDEX_NAME")


class Crud_elastic:
    def __init__(self, elastic_url):
        self.es = Elasticsearch(elastic_url)
        self.index_name = indices_name

    @staticmethod
    def create_mapping(mapping=None):
        if mapping is None:
            mapping = {
                    "properties": {
                        "TweetID": {"type": "keyword"},
                        "CreateDate": {"type": "keyword"},
                        "Antisemitic": {"type": "keyword"},
                        "text": {"type": "text",
                            "fields": {"raw": {"type": "keyword"}}},
                        "score":{"type":"keyword"},
                        "weapons_list":{"type":"keyword"}
                        }}
        return mapping

    def create_index(self):
        try:
            if self.es.indices.exists(index=self.index_name):
                self.es.indices.delete(index=self.index_name)
            if not self.es.indices.exists(index=self.index_name):
                self.es.indices.create(index=self.index_name,mappings=self.create_mapping())

            mapping = self.es.indices.get_mapping(index=self.index_name)
            print(mapping)
            return mapping
        except Exception as e:
            raise e

    def insert_all(self,list_of_documents):
        try:
            if not isinstance(list_of_documents,list) and len(list_of_documents) > 0:
                list_of_documents = list(list_of_documents)
            insert_one = [{"_index": self.index_name,"_id": index+1,"_source":doc}
            for index ,doc in enumerate(list_of_documents)]

            helpers.bulk(self.es,insert_one )
            self.es.indices.refresh(index=self.index_name)
            return self.es.count()
        except Exception as e:
            raise e


    def search_by_query(self,query=None):
        if query is None:
            all_documents = [hit  for hit in scan(self.es, query={"query":{"match_all":{}}},_source=True, index=self.index_name)]
            # print(all_documents)
        else:
            all_documents = self.es.search(index=self.index_name,query=query)
        return all_documents

    def delete_by_search_or_id(self,list_to_delete,query=None,doc_id=None):
        try:
            if list_to_delete:
                actions = [{"_op_type": "delete",
                        "_index": self.index_name,
                        "_id": doc_id}
                for doc_id in list_to_delete]
                success, errors = helpers.bulk(self.es, actions)
                self.es.indices.refresh(index=self.index_name)
                return f"{success} is deleted {errors} not deleted"
            if list_to_delete is None and query is not None and id is None:
                doc_id = self.search_by_query(query)
            res_delete = self.es.delete(index=self.index_name,id=doc_id)
            return f"{doc_id} is deleted {res_delete}"
        except Exception as e:
            raise e

    def delete_all(self):
        self.es.indices.delete(index=self.index_name)

    def update_document(self,list_to_update:list,query=None,doc_id=None,update_doc=None):
        try:

            actions = [
                {"_op_type": "update",
                    "_index": self.index_name,
                    "_id": doc_id_apdate,
                    "doc": doc_fields}
                for doc_id_apdate, doc_fields in list_to_update
                ]
            success,errors = helpers.bulk(self.es, actions)

            # if list_to_update is None and doc_id is None and query is not None:
            #     doc_id = self.search_by_query(query)
            # res_update = self.es.update(index=self.index_name, id=doc_id, body=update_doc)
            return f"{success} is updated {errors} is not updated"
        except Exception as e:
            raise e

