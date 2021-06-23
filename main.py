# Matrikelnummern: 1005644, 7714518, 1532789

import data_cleaning
import pymongo
import pandas as pd
import collections
import tkinter as tk

class SearchQuery():

    pass
    


if __name__ == "__main__":
    # data_cleaning()

    data = pd.read_csv(data_cleaning.out_data_path, delimiter=",")

    d = data.to_dict(orient="index")
       

    myclient = pymongo.MongoClient("mongodb://localhost:27017")
    mydb = myclient["Netflix_Titles"]
    mycol = mydb["Title_Database"]

    # print(d.items())
    # for v in d.values():
    #     mycol.insert_one(v)

    # print(mycol.find_one({"title":"42 Grams"}))
    for posts in mycol.find({"director": "Steven Spielberg"}):
        print(posts["title"])