"""
Matrikelnummern: 1005644, 7714518, 1532789
"""
import os
import json
# import data_cleaning     # commented out since it would take a lot of time to finish, clean data is provided.
import pymongo
import pandas as pd

class SearchQuery():
    """
    Search Query serves as the main class for any search that is conducted.
    It establishes the database connection and handles the user input, does sanity
    checks and searches for the given input in the MongoDB Database.

    """
    CLIENT = pymongo.MongoClient("mongodb://localhost:27017")
    DATABASE = CLIENT["Netflix_Titles"]
    COLUMN = DATABASE["Title_Database"]

    __query_history = [] # holds searches the user inputs
    __user_exit = 0 # variable to exit the program if the user choses to

    # dialog dict maps the search options to the column name in the database

    dialog_dict ={  1: {"message": "\nName of the title: ", "column": "title"},
                    2: {"message": "\nWhat type you want to search, movie or TV show: ", "column": "type"},
                    3: {"message": "\nDirector: ", "column": "director"}, 
                    4: {"message": "\nActress or actor: ", "column": "cast"},
                    5: {"message": "\nCountry: ", "column": "country"}, 
                    6: {"message": "\nRelease year: ", "column": "release_year"},
                    7: {"message": "\nRating: ", "column": "rating"},
                    8: {"message": "\nGenre: ", "column": "listed_in"},
                    }

    main_menue =  """ 

    _   __       __   ____ __ _          ____          __          __                     
   / | / /___   / /_ / __// /(_)_  __   / __ \ ____ _ / /_ ____ _ / /_   ____ _ _____ ___ 
  /  |/ // _ \ / __// /_ / // /| |/_/  / / / // __ `// __// __ `// __ \ / __ `// ___// _ \\
 / /|  //  __// /_ / __// // /_>  <   / /_/ // /_/ // /_ / /_/ // /_/ // /_/ /(__  )/  __/
/_/ |_/ \___/ \__//_/  /_//_//_/|_|  /_____/ \__,_/ \__/ \__,_//_.___/ \__,_//____/ \___/ 
                                                                by Shara, Timo and Andi
    
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

        # print out the main menue
        print(self.main_menue) 

        # if previous searches exist, add the search history to the menue.
        if len(self.getQueryHistory()):
            print("Last three searches: \n")
            print(self.searchHistoryString())

        
    def getQueryHistory(self):
        return SearchQuery.__query_history
    
    def searchHistoryString(self):
        history_string = ""
        # get the last three searches from the __query_history class variable
        for search_dicts in self.getQueryHistory()[-3:]: 
                for k, v in search_dicts.items():
                    history_string += f"{k} : {v} \n"
        return history_string 

    def getQueryDict(self):
        return self.query_dict
        
    def getUserExit(self):
        return self.__user_exit
    
    def setUserExit(self):
        self.__user_exit = 1

    # getQueryCode() gets the user input, checks if the input is valid and saves the queryCode
    def getQueryCode(self):
        self.query_code = input("\nWhich filter(s) do you want to use? Type in the numbers (eg. 245): ")
        # 9 is not an option, delete it from the string
        if "9" in self.query_code:
            self.query_code = ''.join([d for d in self.query_code if d != "9"])
            print("NEIN 9 NINE!!!!!")
        # non numeric values are not allowed
        if not self.query_code.isnumeric():
            print("Please only enter numbers which are valid")
            self.getQueryCode()
        # zero in an otherwise valid input would mean exit at the same time as a search
        if "0" in self.query_code and len(self.query_code) > 1:
            print("Can't search and exit at the same time!")
            self.getQueryCode()
        # zero as only input ends the program
        if "0" in self.query_code and len(self.query_code) == 1:
            self.exitApplication()

        # throw out duplicates since all search terms can be given at once per search category
        self.query_code = set(self.query_code)

    # generateSearchQuery() will generate fitting search queries for MongoDB as well as 
    # save the user input in search_summary and append it to __query_history
    def generateSearchQuery(self): 
        search_summary = {}
        # since we can input a specific year or a time window, this input is handled seperately.
        for i in self.query_code:
            i = int(i)
            if 6 == i:

                choice = input("Only search for a specific year? y/n ")
                if choice == "n":
                    lower_date = int(input("From: "))
                    upper_date = int(input("To: "))
                    time_frame = {'$gte': lower_date, '$lte': upper_date}
                    # add filter to the whole search query
                    search_summary.update({self.dialog_dict[i]["column"]: f'{lower_date} "-" {upper_date}'}) 

                elif choice == "y":
                    time_frame = int(input("Release year: "))
                    search_summary.update({self.dialog_dict[i]["column"]: time_frame})

                else:
                    print("Wrong Input")
                    exit() 
                # add filter to the whole search query
                self.query_dict.update({"release_year": time_frame}) 
                
            else:
                filter_input = input(self.dialog_dict[i]["message"])
                # add filter to the whole search query
                self.query_dict.update(
                    {self.dialog_dict[i]["column"] : {'$regex':filter_input, '$options':'i'}}
                    )
                search_summary.update({self.dialog_dict[i]["column"]: filter_input})

        SearchQuery.__query_history.append(search_summary) 

    # getTitles() will conduct the search in the Database and return all found Titles in chunks of 10
    # if there are more than 10 Titles.
    # The user is asked, if he wants to display the next titles. If not, or the end is reached, a question
    # is prompted, if a new search should be started. 
    def getTitles(self):
        result = self.COLUMN.find(self.query_dict) # actual search in DB

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

    # Function to execute all necessary steps for a search           
    def runApplication(self):
        
        self.getQueryCode()
        self.generateSearchQuery()
        self.getTitles()

    # exit application gracefully
    def exitApplication(self):
        self.setUserExit()
        print("Thank you for using our Netflix Database")
        print("Exiting program...")
        exit()


# Initialize Database with cleaned data from 'netflix_titles_cleaned.csv'.
# Checks if Database is already initialized in the 'flags.json' file. If so, 
# it's skipped, if not, the database is initialized.
def initializeDatabase():
    flags = os.path.abspath("flags.json")
    print(flags)
    f = open(flags, "r")
    json_init = json.load(f)
    f.close()

    #check if DB is already initialized
    if json_init["INITIALIZED"] == 0:
        data_in = os.path.abspath("data/netflix_titles_cleaned.csv")
        data = pd.read_csv(data_in, delimiter=",")
        d = data.to_dict(orient="index")
        myclient = pymongo.MongoClient("mongodb://localhost:27017")
        mydb = myclient["Netflix_Titles"]
        mycol = mydb["Title_Database"]

        for v in d.values():
            mycol.insert_one(v)

        print("Database initialized")

        # set flag for an already initialized DB
        json_init["INITIALIZED"] = 1
        f = open(flags, "w")
        json.dump(json_init, f, indent=4)
        f.close()

    else: 
        pass


if __name__ == "__main__":

    # data_cleaning is commented out here as well as in the import. The cleaning with omdb
    # takes a long time and the cleaned csv is provided.
    # data_cleaning()
    
    initializeDatabase()
    query = SearchQuery()

    while query.getUserExit() == 0:
        if len(query.getQueryHistory()) != 0:
            query = SearchQuery()
        query.runApplication()
        new_search = input("New search (y/n)? ")
        
        if new_search == "y":
            continue
        else:
           query.exitApplication()