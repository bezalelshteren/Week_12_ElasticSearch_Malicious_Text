import nltk
from dotenv import load_dotenv
import os
NLTK_PATH = os.path.join(os.getcwd(),"../nltk_data")
nltk.data.path.append(NLTK_PATH)
from nltk.sentiment.vader import SentimentIntensityAnalyzer
sentimentIntensityAnalyzer = SentimentIntensityAnalyzer()
load_dotenv()



class Processor:
    def __init__(self,weapons_list):
        self.documents = None
        self.weapons_list = weapons_list


    def is_positive_or_negative(self,document:dict):
        score = sentimentIntensityAnalyzer.polarity_scores(document["text"])["compound"]
        if score >= 0.5:
            return "positive"
        if score < -0.5:
            return "negative"
        else:
            return "normal"


    def check_if_their_is_weapons(self,document:dict):
        list_of_weapons = []
        for weapon in self.weapons_list.split(" "):
            if weapon in document["text"]:
                list_of_weapons.append(weapon)
        return list_of_weapons

    def start_all_process_to_update(self,list_documents):
        self.documents = list_documents
        list_to_update = [
            (doc["_id"],
                {"score": self.is_positive_or_negative(doc["_source"]),
                "weapons_list": self.check_if_their_is_weapons(doc["_source"])})
            for doc in self.documents]

        return list_to_update


# "*"
# from crub_elastic import Crud_elastic
# from data_loader import Dal
# path_csv = os.getenv("CSV_PATH")
# d = Dal()
# iii = d.read_from_file_to_json(path_csv)
# print(iii)
# elasticsearch_url = os.getenv("ELASTIC_URL")
# c = Crud_elastic(elasticsearch_url)
# c.insert_all(iii)
# "*"
