




class Delete_not_relevant:
    def __init__(self):
        pass


    def check_if_delete_the_document(self, documents):
        list_to_delete = (
            doc["_id"]
            for doc in documents
            if doc["_source"].get("Antisemitic") == '0' and
               len(doc["_source"].get("weapons_list", [])) == 0 and
               doc["_source"].get("score") in ["positive", "normal"])
        return list_to_delete