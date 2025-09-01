import csv
import os
from dotenv import load_dotenv
load_dotenv()

path_csv = os.getenv("CSV_PATH")
weapons_path = os.getenv("WEAPONS_LIST_PATH")


class Dal:
    def __init__(self):
        self.list_weapons = None
        self.csv_like_a_json = None

    def read_from_file_to_json(self,path):
        with open(path,mode="r", encoding="utf-8") as file:
            self.csv_like_a_json = list(csv.DictReader(file))
        return self.csv_like_a_json

    def read_list_weapons(self,list_weapons_path):
        with open(list_weapons_path,mode="r")as file:
            self.list_weapons = file.read()
        return self.list_weapons


# d = Dal()
# d.read_list_weapons(weapons_path)
# d.read_from_file_to_json(path_csv)