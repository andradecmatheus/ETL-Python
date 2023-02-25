# %%
#!/usr/bin/env python
# coding: utf-8

# importing modules

import pandas as pd
import requests
import json
import config

# %% [markdown]
# ## Extract

# %%
# For this exercise, we’re going to request 6 movies with movie_id ranging from 550 to 555. We create a loop that requests each movie one at a time and appends the response to a list.

response_list = []
API_KEY = config.api_key

# send a single GET request to the API. In the response, we receive a JSON record with the movie_id's we specify
for movie_id in range(550,556):
    r = requests.get('https://api.themoviedb.org/3/movie/{}?api_key={}'.format(movie_id, API_KEY))
    response_list.append(r.json()) 

# %%
# We now have a list of long, unwieldy JSON records delivered to us from the API.
print(type(response_list))

# Creating a pandas dataframe from the records using from_dict()
df = pd.DataFrame.from_dict(response_list)

# %%
# Now, we have the extract data
df

# %% [markdown]
# ## Transform

# %% [markdown]
# The genres column is a column of lists of JSON records, which is hard to read or quickly understand in this format. We want to expand this column out so we can easily see and make use of the internal records.

# %%
genres_list = df['genres'].tolist()

# It is a list of a dictionaries lists
#genres_list

# Example 1
# tree dictionaries in one list
# List1[{Dic1},
#       {Dic2},
#       {Dic3}]
# So, it is a list of dictionaries

# Example 2 (our case)
# tree dictionaries in one list
# List1[
#   SubList1[{Dic1},
#            {Dic2},
#            {Dic3}],
#   SubList2[{Dic1},
#            {Dic2},
#            {Dic3}],
#   SubList3[{Dic1},
#            {Dic2},
#            {Dic3}]]

# So, it is a list of a dictionaries lists
# or it is a list of a list of dictionaries

# %%

# fruits = ["apple", "banana", "cherry", "kiwi", "mango"]
# newlist = [x for x in fruits if "a" in x] 
# for x in fruits: 
#   if "a" in x: 
#       newlist.append(x)
# https://www.w3schools.com/python/python_lists_comprehension.asp


# Since, genres_list is a regular List of Lists (2d_list), so the codes below both work for this case:
# create a flat list based on an existing 2D list
flat_list = [item for sublist in genres_list for item in sublist]

# above is the same as below
# newlist = []
# for sublist in genres_list: # I take 
#     #newlist.append(sublist) # Example 2 (our case)
#     for item in sublist:
#         newlist.append(item) # Example 1
# newlist 
# have in mind that flat_list is equal to newlist

#flat_list

# for to know more about what is a flat list:
# https://stackabuse.com/python-how-to-flatten-list-of-lists/

# %%
#list(flat_list[0].values())[1]
# dict.values()[index]

# %%
# Creating a separate table for genres
df_genres = pd.DataFrame.from_records(flat_list).drop_duplicates()
df_genres

# This gives us a table of the genre properties name and id

# %% [markdown]
# Creating a column of lists to explode out. 
# 

# %%
result = []
for l in genres_list:
    r = []
    for d in l:
        r.append(d['name'])
    result.append(r)

# creating a temporary column, genres_all, as a list of lists of genres that we can later expand out into a separate column for each genre    
df = df.assign(genres_all=result)
# dataframe.assign() - returning a new object (a copy) with the new columns added to the original ones.

# %%
#result

# %%
#df['genres_all']

# %%
# for l in genres_list:
#     r = []
#     for d in l:
#         r.append(d['name'])
#     result.append(r)

#flat_list = [item for sublist in genres_list for item in sublist]

#res_new = [d['name'] for l in genres_list for d  in l]   
#res_new 
# it is different from above

# %%
#df

# %%
# to select the columns we want from the main dataframe
df_columns = ['budget', 'id', 'imdb_id', 'original_title', 'release_date', 'revenue', 'runtime']

# creating a list of genres from that df_genres
df_genre_columns = df_genres['name'].to_list()

# adding the genres as columns
df_columns.extend(df_genre_columns)

# %%
#  to create the genre columns
s = df['genres_all'].explode() # Exploded lists to rows; index will be duplicated for these rows.

#  and join them onto the main table.
df = df.join(pd.crosstab(s.index, s))

# %%
# to understand it better

# it is a Series Object
#print(s) 

# crosstab - by default computes a frequency table of the factors 
#pd.crosstab(index = s.index, columns = s)

# %% [markdown]
# One-hot encoding: It was exploded out the column of lists into one-hot categorical columns. This was done by creating a single column for each categorical value (genres) and setting the row value to 1 if the movie belongs to that category and 0 if it doesn’t. 

# %%
df[df_columns]

# %% [markdown]
# Notice the genre columns to the right. If a movie belongs to a genre, the row value is 1, and if not, the value is 0. Now it’s easy for us to filter on specific genres and to quickly tell if a movie belongs to a genre or not.

# %% [markdown]
# ### Working with datetimes
# Finally we’ll expand out the datetime column into a table. Pandas has built-in functions to extract specific parts of a datetime. Notice we need to convert the release_date column into a datetime first.

# %%
df['release_date'] = pd.to_datetime(df['release_date'])

# creating columns relating to datetime
df['day'] = df['release_date'].dt.day
df['month'] = df['release_date'].dt.month
df['year'] = df['release_date'].dt.year
df['day_of_week'] = df['release_date'].dt.day_name()

# %%
df_time_columns = ['id', 'release_date', 'day', 'month', 'year', 'day_of_week']

df[df_time_columns]

# %% [markdown]
# ## Load
# We ended up creating 3 tables for the tmdb schema that we’ll call movies, genres, and datetimes. 
# We export our tables by writing them to file. This will create 3 .csv files in the same directory that our script is in.

# %%
df[df_columns].to_csv('tmdb_movies.csv', index=False)
df_genres.to_csv('tmdb_genres.csv', index=False)
df[df_time_columns].to_csv('tmdb_datetimes.csv', index=False)

# %%
# df

# %% [markdown]
# That’s it! We’ve created our first ETL pipeline. <br><br> We built a structured schema from a list of JSON records and transformed our data into a clean, usable format. <br>You can experiment with many different ways to explore and structure this data, as we have only scratched the surface here and used a small part of what we had available.

# %% [markdown]
# # Source
# 
# https://towardsdev.com/create-an-etl-pipeline-in-python-with-pandas-in-10-minutes-6be436483ec9
# 
# https://github.com/habibdraft/tmdb/blob/main/tmdb.py
# 
# <br>
# 
# 
# ## Relevant comments 
# - This is not really an ETL, but you may improve your work using the pipe() function from pandas.


