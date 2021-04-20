#!/usr/bin/env python3

'''
AWS:
1. Dynamodb
2. Some Analytic thing
3. Athena
4. Quicksight

Workflow:
1. Send a request for movie metadata to OMDB
2. OMDB returns a response
3. Potentially parse data into DB-schema
4. Send transformed data to S3 / Send transformed data to Dynamodb
5. Athena / Quicksight Magic

Movie API Request:
Gather data based on IMDB ID, iterate through ID numbers to grab a certain number of movie records.
Start with 20 records
Record IMDB ID into DynamoDB Table with movie metadata and use greatest Record ID to pick up where we left off.

- ContentID = integer - for mathametical operation on IMDB ID
- IMDB_ID = Concatenate Movie ID with IMDB ID Prefix, e.g. "tt" - gets full string
- Request URL = URL + IMDB_ID + API Key

API Key reads from environment variable API_KEY

"What's stopping me from building a planet with oceans of mercury?"

content_id_start = integer - perform operations, get final output
new_url = (f'{content_id_start:06}')

increment_id
1. Separated "tt" from number/string
2. Convert string to int
3. Increment int
4. Convert back to string
5. string.zfill(7-len(string))

Example URL:
http://www.omdbapi.com/?i=tt0000004

where 'i' retrieves movie by IMDB ID

API Key Example URL:
http://www.omdbapi.com/?i=tt0000004&apikey=57c4a427

'''



import os
import requests
import boto3

api_url = "http://www.omdbapi.com/"
api_key = os.getenv('API_KEY')

# Not implemented yet
def get_movie_data(start_id, end_id):
    start_id = "tt0000001"
    end_id = "tt0000020"
    content_id_start = int(start_id.strip("tt"))
    content_id_end = int(end_id.strip("tt"))

# Takes an Content ID from the database, passes it as the "start" for the increment.
# Turns Content ID into an integer and strips out tt, e.g. ContentID is tt0000002
def increment_id(start_id, increment_amount):
    id_list = []
    inc_start_id = int(start_id.strip("tt"))
    i = 0
    while i < increment_amount:
        inc_start_id += 1
        inc_start_id_str = str(inc_start_id)
        inc_start_id_str_zero = inc_start_id_str.zfill(7)
        id_list.append("tt" + inc_start_id_str_zero) 
        i += 1

    return id_list

# Crafts list of URLs for requesting, with IMDB ID embedded. Doesn't handle API Keys, which will be part of request function
def url_generator(id_list,url):
    url_list = []
    for item in id_list:
        url_list.append(url + '?i=' + item)

    return url_list

# Iterates through the previously-generated URL list, adding the appropriate API key in order to request the content. 
# Returns a JSON-dictionary list. Each movie is a list index, represented as a JSON object.
def request_movie(url_list, api_key):
    response_list = []
    for item in url_list:
        print(item + "&" +'apikey={}'.format(api_key))
        response = requests.get(item + "&" +'apikey={}'.format(api_key))
        response_list.append(response.json())
    return response_list
        
# Main run    
id_list = increment_id("tt0000000", 20)
url_list = url_generator(id_list, api_url)
movie_content = request_movie(url_list, api_key)