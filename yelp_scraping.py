import requests
import numpy as np
import datetime


payload = {}
headers = {
    'Authorization': 'Bearer {ACCESS_TOKEN}'
} 

cuisines = ["Chinese", "Indian", "Japanese", "Mexican", "American", "French", "Italian", "Greek", "Korean", "Mediterranean"]
set_restaurants_data = set()
time = set()
restaurants_data = []
max_count_of_cuisines = 1300

for cuisine in cuisines:
    url = "https://api.yelp.com/v3/businesses/search?location=Manhattan&term="+cuisine+"&limit=1&offset=0"
    response = requests.request("GET", url, headers=headers, data=payload)
    if response.status_code != 200:
        print(cuisine)
        break
    cuisine_data = response.json()
    count = cuisine_data.get("total", [])
    limit = 50
    if count > max_count_of_cuisines:
        count = max_count_of_cuisines
    num_offsets = count//limit
    url_temp = "https://api.yelp.com/v3/businesses/search?location=Manhattan&term="+cuisine+"&limit="
    print(cuisine)
    for i in range(num_offsets):
        print(i)
        url = url_temp + str(limit) + "&offset=" + str(i*limit)
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code != 200:
            print(cuisine)
            break
        cuisine_data = response.json()

        space = " "
        for restaurant_data_cuisine in cuisine_data.get("businesses", []):
            restaurant_data = {}
            restaurant_data["instertedAtTimestamp"] = str(datetime.datetime.now().timestamp())
            restaurant_data["Cuisine"] = cuisine
            try:
                restaurant_data["Name"] = restaurant_data_cuisine["name"]
            except:
                restaurant_data["Name"] = "Name not available"
            try:
                restaurant_data["Business ID"] = restaurant_data_cuisine["id"]
            except:
                restaurant_data["Business ID"] = "Business ID not available"
            try:
                restaurant_data["Address"] = space.join(restaurant_data_cuisine["location"]["display_address"])
            except:
                restaurant_data["Address"] = "Address not available"
            try:
                restaurant_data["Latitude"] = restaurant_data_cuisine["coordinates"]["latitude"]
            except:
                restaurant_data["Latitude"] = "Latitude not available"
            try:
                restaurant_data["Longitude"] = restaurant_data_cuisine["coordinates"]["longitude"]
            except:
                restaurant_data["Longitude"] = "Longitude not available"
            try:
                restaurant_data["Zip Code"] = restaurant_data_cuisine["location"]["zip_code"]
            except:
                restaurant_data["Zip Code"] = "Zip Code not available"
            try:
                restaurant_data["Number of reviews"] = restaurant_data_cuisine["review_count"]
            except:
                restaurant_data["Number of reviews"] = "Number of reviews not available"
            try:
                restaurant_data["Rating"] = restaurant_data_cuisine["rating"]
            except:
                restaurant_data["Rating"] = "Rating not available"
            try:
                restaurant_data["Price"] = restaurant_data_cuisine["price"]
            except:
                restaurant_data["Price"] = "Price details not available"
            try:
                restaurant_data["Availability"] = space.join(restaurant_data_cuisine["transactions"])
            except:
                restaurant_data["Availability"] = "Not available right now"
            try:
                restaurant_data["Phone Number"] = restaurant_data_cuisine["phone"]
            except:
                restaurant_data["Phone Number"] = "Phone Number not available"
            try:
                restaurant_data["Distance"] = restaurant_data_cuisine["distance"]
            except:
                restaurant_data["Distance"] = "Distance not available"
            if restaurant_data_cuisine["id"] not in set_restaurants_data:
                set_restaurants_data.add(restaurant_data_cuisine["id"])
                restaurants_data.append(restaurant_data)
                time.add(str(restaurant_data["instertedAtTimestamp"]))


print(len(restaurants_data))
print(len(set_restaurants_data))
print(len(time))
#

file_path = "example.txt"

with open(file_path, "w") as file:
    for line in restaurants_data:
        file.write(str(line))
        file.write('\n')
