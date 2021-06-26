"""
Matrikelnummern: 1005644, 7714518, 1532789
"""

import json
from re import search
# import data_cleaning
import pymongo
import pandas as pd

class SearchQuery():

    # search for: Movie/TV-Show, Actor, Country, Director, Genres, recently added (newest 10), rating, release year,...
    # search for: date range (combinded with simple searches)
    
    CLIENT = pymongo.MongoClient("mongodb://localhost:27017")
    DATABASE = CLIENT["Netflix_Titles"]
    COLUMN = DATABASE["Title_Database"]

    __query_history = []
    __user_exit = 0

    dialog_dict ={  1: {"message": "\nName of the title: ", "column": "title"},
                    2: {"message": "\nWhat type you want to search, movie or TV show: ", "column": "type"},
                    3: {"message": "\nDirector: ", "column": "director"}, 
                    4: {"message": "\nActress or actor: ", "column": "cast"},
                    5: {"message": "\nCountry: ", "column": "country"}, 
                    6: {"message": "\nRelease year: ", "column": "release_year"},
                    7: {"message": "\nRating: ", "column": "rating"},
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

        0) exit

    """

    def __init__(self):

        self.query_dict = {}
        self.query_code = ""

        print(self.search_options_string)

        if len(self.getQueryHistory()):
            print("Search History")
            print(self.searchHistoryString())

        
    def getQueryHistory(self):
        return SearchQuery.__query_history
    
    def searchHistoryString(self):
        history_string = ""
        for num, search_dicts in enumerate(self.getQueryHistory()):
            if num == (len(self.getQueryHistory()) - 3) or num <= 3:
                history_string += f"{num+1}. \n"
                cache_string = ""
                for k, v in search_dicts.items():
                    cache_string += f"{k} : {v} \n" 
                history_string += cache_string + "\n"
            else:
                pass
        return history_string 

    def getQueryDict(self):
        return self.query_dict
        
    def getUserExit(self):
        return self.__user_exit
    
    def setUserExit(self):
        self.__user_exit = 1

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
        search_summary = {}
        self.query_code = input("\nWhich filter(s) do you want to use? Type in the numbers (eg. 245): ")
        if not self.query_code.isnumeric():
            print("Please enter only numbers")
            self.getQueryCode()

        if "0" in self.query_code and len(self.query_code) > 1:
            print("Can't search and exit at the same time!")
            self.getQueryCode()
        
        if "0" in self.query_code and len(self.query_code) == 1:
            self.exitApplication()

        for i in self.query_code:
            
            i = int(i)
            if 6 == i:

                choice = input("Only search for a specific year? y/n ")
                if choice == "n":
                    lower_date = int(input("From: "))
                    upper_date = int(input("To: "))
                    time_frame = {'$gt': lower_date, '$lt': upper_date}

                    search_summary.update({self.dialog_dict[i]["column"]: f'{lower_date} "-" {upper_date}'})

                elif choice == "y":
                    time_frame = int(input("Release year: "))
                    search_summary.update({self.dialog_dict[i]["column"]: time_frame})

                else:
                    print("Wrong Input")
                    exit() 

                self.query_dict.update({"release_year": time_frame})
                
            else:
                filter_input = input(self.dialog_dict[i]["message"])
                self.query_dict.update(
                    {self.dialog_dict[i]["column"] : {'$regex':filter_input, '$options':'i'}}
                    )
                search_summary.update({self.dialog_dict[i]["column"]: filter_input})

        SearchQuery.__query_history.append(search_summary) 
          
    def run_application(self):
        
        self.getQueryCode()
        
        self.getTitle()

    def exitApplication(self):
        self.setUserExit()
        print("Thank you for using our Netflix Database")
        print("Exiting program...")
        exit()



def initializeDatabase():
    flags = "./flags.json"
    f = open(flags, "r")
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
        f = open(flags, "w")
        json.dump(json_init, f, indent=4)
        f.close()

    else: 
        pass

if __name__ == "__main__":

    # data_cleaning()
    initializeDatabase()
    query = SearchQuery()



       
    while query.getUserExit() == 0:
        if len(query.getQueryHistory()) != 0:
            query = SearchQuery()
        query.run_application()
        new_search = input("New search (y/n)? ")
        
        if new_search == "y":
            continue
        else:
           query.exitApplication()
        
    # print(query.getQueryHistory())

