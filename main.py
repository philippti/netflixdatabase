import data_cleaning
import pymongo
import pandas as pd
import collections


class SearchQuery():

    pass
    








if __name__ == "__main__":

    in_data_path = "./data/netflix_titles_cleaned.csv"
    data = pd.read_csv(in_data_path, delimiter=",", index_col="show_id")

    d = data.to_dict(orient="index")
       

    myclient = pymongo.MongoClient("mongodb://localhost:27017")
    mydb = myclient["Netflix_Titles"]
    mycol = mydb["Title_Database"]

    for k, v in d.items():
        mycol.insert_one({k: v})


    # # data_cleaning()


    # print(d['s1'])

