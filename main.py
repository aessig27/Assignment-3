import requests
import json
import matplotlib.pyplot as plt
import numpy as np
import redis.commands.search.aggregation as aggregations
from db_config import get_redis_connection
from redis.commands.search.field import TextField, NumericField, TagField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
from redis.commands.search.query import NumericFilter, Query

r = get_redis_connection()

def main():
    
    def json_api(url, headers = None, query_params = None):

        response = requests.get(url, headers = headers, params = query_params)
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Failed to fetch data from API. Status code: {response.status_code}")
            return None

    url = "https://nhl-api5.p.rapidapi.com/nhlteamplayers"

    querystring = {"teamid":"15"}

    # insert unique API Key
    headers = {
        "X-RapidAPI-Key": "api_key",
        "X-RapidAPI-Host": "nhl-api5.p.rapidapi.com"
    }

    json_data = json_api(url, headers = headers, query_params = querystring)
    if json_data:
        print(json_data)

        r.json().set('api_data', '.', json.dumps(json_data))

        print()
        print("JSON data inserted into Redis.")
        print()
    else:
        print("Failed to insert JSON data into Redis.")

def retrieve_data():
    
    json_data = r.json().get('api_data')    
    data = json.loads(json_data).get('team', {}).get('athletes', [])

    if data:
        athletes = data[0:31]

        for player in athletes:
            name = player.get('fullName')
            position = player.get('position', {}).get('name')
            age = player.get('age')

            print(f"Name: {name}")
            print(f"Position: {position}") 
            print(f"Age: {age}")
            print()

        player_data = {
            'name': name,
            'position': position,
            'age': age
        }

        r.json().set('athlete_data', '.', json.dumps(player_data))

        print("Player data inserted into Redis.")
        print()
    
    else:
        print("No player data found.")

    json_data = r.json().get('api_data')    
    data2 = json.loads(json_data).get('team', {}).get('athletes', [])

    if data2:
        athletes = data[5]
        name = athletes.get('fullName')
        city = athletes.get('birthPlace', {}).get('city')
        country = athletes.get('birthPlace', {}).get('country')

        print(f"Name: {name}")
        print(f"City: {city}")
        print(f"Country: {country}")
        print()

        nationality_data = {
            'name': name,
            'city': city,
            'country': country
        }

        r.json().set('nationality_data', '.', json.dumps(nationality_data))

        print("Nationality data inserted into Redis.")
        print()
        print()
    
    else:
        print("No nationality data found.")

    json_data = r.json().get('api_data')    
    data3 = json.loads(json_data).get('team', {}).get('athletes', [])

    if data3:
        athletes = data[17]
        name = athletes.get('fullName')
        draft = athletes.get('draft', {}).get('displayText')

        print(f"Name: {name}")
        print(f"Draft: {draft}")

        draft_data = {
            'name': name,
            'draft': draft,
        }

        r.json().set('draft_data', '.', json.dumps(draft_data))

        print("Draft data inserted into Redis.")
        print
    
    else:
        print("No draft data found.")

def process_data():

    schema = (TextField("$.position", as_name="position"))

    r.ft().dropindex(schema)

    r.ft().create_index(schema, definition=IndexDefinition(index_type=IndexType.JSON))

    r.ft().search("Right Wing")

    q = Query("*").paging(0, 0)
    r.ft().search(q).total


if __name__ == "__main__":
    main()
    retrieve_data()
