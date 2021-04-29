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


'''
04/28 Notes:

Should we create a Movie class - use movie_attributes as class 
'''

import os
import requests
import boto3

api_url = "http://www.omdbapi.com/"
api_key = os.getenv('API_KEY')
g_ddb_client = boto3.client("dynamodb")

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

def create_table(table_name):
    print("Creating DynamoDB Lock Table...")
    response = g_ddb_client.create_table(
        AttributeDefinitions=[
            {
                'AttributeName': 'imdbID',
                'AttributeType': 'S'
            },
        ],
        TableName='{}'.format(table_name),
        KeySchema=[
            { 
                'AttributeName': 'imdbID',
                'KeyType': 'HASH'
            },
        ],
        BillingMode='PAY_PER_REQUEST'
    )
    check = g_ddb_client.describe_table(TableName='terraform')
    '''
    CreateTable is an asynchronous operation. Upon receiving a CreateTable request, DynamoDB immediately returns a response with a TableStatus of CREATING . 
    After the table is created, DynamoDB sets the TableStatus to ACTIVE . You can perform read and write operations only on an ACTIVE table.
    '''
    # Check for ACTIVE status table after creation.
    while True:
        print("Polling for table creation status to go to Active...\n")
        check = g_ddb_client.describe_table(TableName='terraform')
        if check['Table']['TableStatus'] == 'ACTIVE':
            break
        time.sleep(3) 
    print('Table created!\n')
    return table_name

def update_table(content,table_name):
    for item in content:
        response = g_ddb_client.update_item(
        TableName='{}'.format(table_name),
        Key={
            'imdbID': {'S': '{}'.format(item['imdbID'])}
        },
        UpdateExpression='SET Title = :title, \
            MovieYear = :year, \
            Rated = :rated, \
            Released = :released, \
            Runtime = :runtime, \
            Genre = :genre, \
            Director = :director, \
            Writer = :writer, \
            Actors = :actors, \
            Plot = :plot, \
            Lang = :lang, \
            Country = :country, \
            Awards = :awards, \
            Poster = :poster, \
            Metascore = :metascore, \
            imdbRating = :imdbrating, \
            imdbVotes = :imdbvotes, \
            Content_Type = :content_type, \
            DVD = :dvd, \
            BoxOffice = :boxoffice, \
            Production = :production, \
            Website = :website',
        ExpressionAttributeValues={
            ':title': {'S': '{}'.format(item['Title'])},
            ':year': {'S': '{}'.format(item['Year'])},
            ':rated': {'S': '{}'.format(item['Rated'])},
            ':released': {'S': '{}'.format(item['Released'])},
            ':runtime': {'S': '{}'.format(item['Runtime'])},
            ':genre': {'S': '{}'.format(item['Genre'])},
            ':director': {'S': '{}'.format(item['Director'])},
            ':writer': {'S': '{}'.format(item['Writer'])},
            ':actors': {'S': '{}'.format(item['Actors'])},
            ':plot': {'S': '{}'.format(item['Plot'])},
            ':lang': {'S': '{}'.format(item['Language'])},
            ':country': {'S': '{}'.format(item['Country'])},
            ':awards': {'S': '{}'.format(item['Awards'])},
            ':poster': {'S': '{}'.format(item['Poster'])},
    #        ':ratings': {'L': item['Ratings'], 'M': item['Ratings'][item]},
            ':metascore': {'S': '{}'.format(item['Metascore'])},
            ':imdbrating': {'S': '{}'.format(item['imdbRating'])},
            ':imdbvotes': {'S': '{}'.format(item['imdbVotes'])},
            ':content_type': {'S': '{}'.format(item['Type'])},
            ':dvd': {'S': '{}'.format(item['DVD'])},
            ':boxoffice': {'S': '{}'.format(item['BoxOffice'])},
            ':production': {'S': '{}'.format(item['Production'])},
            ':website': {'S': '{}'.format(item['Website'])},
        },
        ReturnValues="UPDATED_NEW"
        )


# Create list of keys in movie response. We'll use this to create our table schema in Dynamo.
def get_movie_attributes(movies):
    attributes = []
    for key in movies.keys():
        attributes.append(key)

    print(attributes)
    return attributes


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
id_list = increment_id("tt0000000", 3)
url_list = url_generator(id_list, api_url)
movie_content = request_movie(url_list, api_key)
movie_attributes = get_movie_attributes(movie_content[0])
#table_name = create_table("movies")
update_table(movie_content,"movies")

