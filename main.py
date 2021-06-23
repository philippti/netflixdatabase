# Matrikelnummern: 1005644, 7714518, 1532789

import json
import data_cleaning
import pymongo
import pandas as pd
class SearchQuery():

    # search for: Movie/TV-Show, Actor, Country, Director, Genres, recently added (newest 10), rating, release year,...
    # search for: date range (combinded with simple searches)
    
    CLIENT = pymongo.MongoClient("mongodb://localhost:27017")
    DATABASE = CLIENT["Netflix_Titles"]
    COLUMN = DATABASE["Title_Database"]

    query_history = []
    user_exit = 0

    dialog_dict ={  1: {"message": "\nName of the title: ", "column": "title"},
                    2: {"message": "\nWhat type you want to search, movie or TV show: ", "column": "type"},
                    3: {"message": "\nDirector: ", "column": "director"}, 
                    4: {"message": "\nActress or actor: ", "column": "cast"},
                    5: {"message": "\nCountry: ", "column": "country"}, 
                    6: {"message": "\nRelease year: ", "column": "release_year"},
                    7: {"message": "\nRating: ", "column": "rated"},
                    8: {"message": "\nGenre: ", "column": "listed_in"},
                    }

    search_options_string =  """ 
    Search options:
        1) Title name
        2) Type of title (Movie or TV Show) 
        3) Director
        4) Actress or actor
        5) Country
        6) Release period
        7) Rating
        8) Genre
    """

    def __init__(self):
        print(self.search_options_string)

        self.query_dict = {}
        self.query_code = input("\nWhich filter do you want to use? Type in the numbers (eg. 245): ")
        
        self.getQueryCode()
        
        self.getTitle()

        SearchQuery.query_history.append(self)

    def getTitle(self):
        result = self.COLUMN.find(self.query_dict)

        for i, x in enumerate(result):
            
            print(f"""
            \t Search result {i+1}:\n
            \t Title: {x['title']}
            \t Type: {x['type']}
            \t Duration: {x['duration']}
            \t Release year: {x['release_year']}
            \t Genre: {x['listed_in']}
            \t Rating: {x['rating']}            
            \t Director: {x['director']}
            \t Cast: {x['cast']}
            \t Country: {x['country']}
            \t Plot summary: {x['description']}
            """)

            if (i+1)%10 == 0 and self.COLUMN.count_documents(self.query_dict) > (i+1):
                next_titles = input("Display next titles? y/n? ")
                if next_titles == 'y':
                    continue
                else:
                    break

    def getQueryCode(self):
         for i in self.query_code:
            i = int(i)
            if 6 == i:

                choice = input("Only search for a specific year? y/n ")
                if choice == "n":
                    lower_date = int(input("From: "))
                    upper_date = int(input("To: "))
                    time_frame = {'$gt': lower_date, '$lt': upper_date}

                elif choice == "y":
                    time_frame = int(input("Release year: "))

                else:
                    print("Wrong Input")
                    exit() 

                self.query_dict.update({"release_year": time_frame})

            else:
                self.query_dict.update({self.dialog_dict[i]["column"] : {'$regex':input(self.dialog_dict[i]["message"]), '$options':'i'}})


def initializeDatabase():
    semaphore = "./semaphor_database.json"
    f = open(semaphore, "r")
    json_init = json.load(f)
    f.close()

    if json_init["INITIALIZED"] == 0:
        data = pd.read_csv(data_cleaning.out_data_path, delimiter=",")
        d = data.to_dict(orient="index")
        myclient = pymongo.MongoClient("mongodb://localhost:27017")
        mydb = myclient["Netflix_Titles"]
        mycol = mydb["Title_Database"]

        for v in d.values():
            mycol.insert_one(v)

        print("Database initialized")

        json_init["INITIALIZED"] = 1
        f = open(semaphore, "w")
        json.dump(json_init, f, indent=4)
        f.close()

    else: 
        pass

if __name__ == "__main__":

    # data_cleaning()
    # initializeDatabase()

    while SearchQuery.user_exit == 0:
        query = SearchQuery()
        new_search = input("New search (y/n)? ")
        
        if new_search == "y":
            continue
        else:
            print("Thank you for using our Netflix Database")
            print("Exiting program...")
            exit()

