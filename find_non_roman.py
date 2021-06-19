#### Source: https://stackoverflow.com/questions/3094498/how-can-i-check-if-a-python-unicode-string-contains-non-western-letters 

import unicodedata as ud
import pandas as pd

data_path = "./data/netflix_titles.csv"
clean_data = pd.read_csv(data_path, delimiter=",", index_col="show_id")

latin_letters= {}

def is_latin(uchr):
    try: return latin_letters[uchr]
    except KeyError:
         return latin_letters.setdefault(uchr, 'LATIN' in ud.name(uchr))

def only_roman_chars(unistr):
    return all(is_latin(uchr)
           for uchr in unistr
           if uchr.isalpha()) # isalpha suggested by John Machin


for items in clean_data["title"]:
    if only_roman_chars(items) == False:
        print(items)

 