import random
from geopy.geocoders import Nominatim
import time

def get_state_city_name(latitude, longitude):
    # Initialize geolocator
    geolocator = Nominatim(user_agent="geoapiExercises")

    # Try to get the location information
    try:
        # time.sleep(random.choice([5, 6, 7]))
        location = geolocator.reverse((latitude, longitude), exactly_one=True)
        address = location.raw['address']

        # Extract state and city names
        state = address.get('state', '')
        city = address.get('city', '')
        
        # print(state, city)
        return state, city

    except Exception as e:
        try:
            time.sleep(random.choice([5, 6, 7]))
            location = geolocator.reverse((latitude, longitude), exactly_one=True)
            address = location.raw['address']

            # Extract state and city names
            state = address.get('state', '')
            city = address.get('city', '')
            print(state, city)

            return state, city    
        except Exception as e:    
            print("Error:", e)
            return None, None
        

from pymongo import MongoClient

# Requires the PyMongo package.
# https://api.mongodb.com/python/current

client = MongoClient('mongodb+srv://thanos_pharma:thanos@thanos.9nxfach.mongodb.net/')
filter={
    "R_state": None, "R_state": {"$exists": True}
}
sort=list({'_id': -1}.items())
# result = 

from tqdm import tqdm
for record in tqdm(client['pharma_data']['pharma'].find(
  filter=filter,
  sort = sort
)):
  record['R_state'], record['R_City'] = get_state_city_name(record['R_Lat'], record['R_Lon'])
  client['pharma_data']['pharma'].update_one(filter={"_id": record['_id']}, update={ "$set": record }, upsert=True)        