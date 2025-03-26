import geopandas as gpd
import json
from pymongo import MongoClient
import os

# MongoDB connection (update the URI if needed)
client = MongoClient("mongodb://localhost:27017/")
db = client["maritime"]  # Update to use the "maritime" database

# List of weather datasets for 2017
weather_datasets = [
    {"name": "jan", "type": "weather"},
    {"name": "feb", "type": "weather"},
    {"name": "mar", "type": "weather"},
    {"name": "apr", "type": "weather"},
    {"name": "may", "type": "weather"},
    {"name": "jun", "type": "weather"},
    {"name": "jul", "type": "weather"},
    {"name": "aug", "type": "weather"},
    {"name": "sep", "type": "weather"},
    {"name": "oct", "type": "weather"},
    {"name": "nov", "type": "weather"},
    {"name": "dec", "type": "weather"}
]

# Path to the folder containing weather shapefiles for 2017
base_path = "C:\\Users\\DIAMAT\\Desktop\\MongoDB\\Data\\Extracted\\noaa_weather\\noaa_weather\\2019"

# Unified collection for all weather data
collection = db["weather_data"]

# Clear the existing collection
# collection.delete_many({})

def read_shapefile(filepath):
    try:
        return gpd.read_file(filepath, encoding='utf-8')
    except UnicodeDecodeError:
        print(f"Warning: UTF-8 decoding failed for {filepath}, falling back to ISO-8859-1")
        return gpd.read_file(filepath, encoding='ISO-8859-1')

for dataset in weather_datasets:
    try:
        print(f"Processing {dataset['name']}...")
        
        # Build the full path to the shapefile
        filepath = os.path.join(base_path, dataset["name"], f"noaa_weather_{dataset['name']}2019_v2.shp")
        
        # Read the shapefile into a GeoDataFrame with explicit encoding handling
        gdf = read_shapefile(filepath)
        
        # Ensure text fields are correctly encoded and stripped of unwanted characters
        for column in gdf.columns:
            if gdf[column].dtype == object:
                gdf[column] = gdf[column].astype(str).str.encode('utf-8', 'ignore').str.decode('utf-8').str.strip()
        
        # Convert the GeoDataFrame to GeoJSON
        gdf_json = json.loads(gdf.to_json())
        
        # Transform data to remove 'Feature' type and flatten properties
        transformed_data = []
        for feature in gdf_json["features"]:
            properties = feature["properties"].copy()
            
            # Extract latitude and longitude
            longitude = feature["geometry"]["coordinates"][0]
            latitude = feature["geometry"]["coordinates"][1]
            
            # Remove lon and lat fields if they exist in properties
            properties.pop("lon", None)
            properties.pop("lat", None)
            
            new_entry = {
                "type": dataset["type"],
                "location": {
                    "longitude": longitude,
                    "latitude": latitude
                },
                "month": dataset["name"]  # Add the month information
            }
            new_entry.update(properties)
            transformed_data.append(new_entry)
        
        # Insert transformed data into the unified collection
        collection.insert_many(transformed_data)
        
        print(f"{dataset['name']} successfully imported into unified collection: weather_data!")
    except Exception as e:
        print(f"Error processing {dataset['name']}: {e}")


print("All weather data from 2017 have been imported into a single collection: weather_data!")
