# Matrikelnummern: 1005644, 7714518, 1532789
import collections
import pandas as pd
import omdb
import json
from datetime import datetime as dt


in_data_path = "./data/netflix_titles.csv"
out_data_path = "./data/netflix_titles_cleaned.csv"

api_key_file = "./api_key.json"
f = open(api_key_file, "r")
api_json = json.load(f)
api_key = api_json["API_KEY"]
f.close()

# Remove all leading and trailing spaces in all string columns
data = pd.read_csv(in_data_path, delimiter=",", index_col="show_id")
for columns in data:
    if columns != "release_year":
        data[columns] = data[columns].str.lstrip()
        data[columns] = data[columns].str.rstrip()

# convert "date_added" to datetime 
data["date_added"] = pd.to_datetime(data["date_added"], format="%B %d, %Y")

clean_dataFrame = pd.read_csv(out_data_path, delimiter=",", index_col="show_id")

# nones = clean_dataFrame[clean_dataFrame.isna().any(axis=1)]
# print(nones)
# print(clean_dataFrame.isna().sum()) #identify the columns with missing data

columns_with_missing_values = ["director", "cast", "country", "rating"]

# get all id's of movies with missing data and save them in a set 

movie_id_missing_values = list()

for columns in columns_with_missing_values:
    temp_list = data[data[columns].isnull()].index.tolist()
    movie_id_missing_values.extend(temp_list)

movie_id_missing_values_set = list(set(movie_id_missing_values))

# fill all nan values with "#####" for easy identification

data.fillna("#####",inplace=True)

# use title and year as identifier for the movie in the open movie data base

key_dictionary = {"director": "Director", "cast": "Actors", "country": "Country", "rating": "Rated"}

counter = 0
for title_id in movie_id_missing_values_set:
    
    title_name = data.loc[title_id]["title"]
    title_release_year = data.loc[title_id]["release_year"]

    # first try to find the title in the OMDB with release year for more accurate identification
    omdb_title_data_with_year = omdb.request(t=title_name, y=int(title_release_year), apikey=api_key)
    omdb_title_data_with_year_json = json.loads(omdb_title_data_with_year.text)

    # if title with given release year ist not found, try to find the title without specifying the release year
    if omdb_title_data_with_year_json['Response'] == 'True':
        omdb_request = omdb_title_data_with_year_json
    else:
        omdb_title_data_without_year = omdb.request(t=title_name, apikey="f1aa0340")
        omdb_request = json.loads(omdb_title_data_without_year.text)

    movie_title = dict(data.loc[title_id])

    if omdb_request["Response"] == "True":
        for k, v in movie_title.items():
            if v == "#####" and k != "date_added":              # "date_added" has to be dealt with seperately since the omdb has no information on the date added to Netflix      
                if omdb_request[key_dictionary[k]] == "N/A":    # replace N/A with "not available" to check if no nan values are left in the DataFrame 
                    data.at[title_id,k] = 'not available'
                else:
                    data.at[title_id,k] = omdb_request[key_dictionary[k]]
                if data.loc[title_id]["type"] == "TV Show" and omdb_request["Director"] == 'not available': # TV-Shows with multiple directors in the series are "N/A" in the OMDB, so here we use "Various Directors"
                    data.at[title_id,"director"] = "Various Directors"
            
            elif v == "#####" and k == "date_added":    # if "date_added" is not in the original data, we have currently no way to correctly fill this field   
                data.at[title_id,k] = "not available" 

    else: # omdb_request["Response"] == "False"
        for k, v in movie_title.items():
            if v == "#####" and k != "date_added":      # "date_added" has to be dealt with seperately since the omdb has no information on the date added to Netflix      
                data.at[title_id,k] = "not found"       # not found in OMDB
                if data.loc[title_id]["type"] == "TV Show" and data.loc[title_id]["director"] == '#####': # TV-Shows with multiple directors in the series are "N/A" in the OMDB, so here we use "Various Directors"
                    data.at[title_id,"director"] = "Various Directors"
    
            elif v == "#####" and k == "date_added":    # if "date_added" is not in the original data, we have currently no way to correctly fill this field   
                data.at[title_id,k] = "not available" 

    counter += 1
    print(f"{round((counter/len(movie_id_missing_values_set)) * 100, ndigits=2)} %") # show progress of fetching the data from OMDB

data.to_csv(out_data_path)     