




class Delete_not_relevant:
    def __init__(self):
        pass

    def check_if_delete_the_document(self,document:dict):
        if document["Antisemitic"]=='0' and \
            len(document["weapons_list"])==0 and \
            (document["score"]=="positive" or document["score"]=="normal"):
            print()