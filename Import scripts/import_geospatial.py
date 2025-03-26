import geopandas as gpd
import json
from pymongo import MongoClient
import os

# MongoDB connection (update the URI if needed)
client = MongoClient("mongodb://localhost:27017/")
db = client["maritime"]  # Update to use the "maritime" database

# List of datasets with their types
geodata = [
    {"name": "harbours", "type": "harbour"},
    {"name": "islands", "type": "island"},
    {"name": "piraeus_port", "type": "port"},
    {"name": "receiver_location", "type": "receiver"},
    {"name": "spatial_coverage", "type": "coverage"},
    {"name": "saronic_territorial_waters", "type": "saronic_territorial_waters"}
]

# Datasets that should be stored as points (polygon centroids)
polygon_to_point = {"islands", "spatial_coverage"}

# Path to the folder containing shapefiles
base_path = r"C:\Users\DIAMAT\Desktop\MongoDB\Data\Extracted\geodata"

# Unified collection for all geodata
collection = db["geodata"]

# Clear the existing collection
collection.delete_many({})

def read_shapefile(filepath):
    try:
        return gpd.read_file(filepath, encoding='utf-8')
    except UnicodeDecodeError:
        print(f"Warning: UTF-8 decoding failed for {filepath}, falling back to ISO-8859-1")
        return gpd.read_file(filepath, encoding='ISO-8859-1')

for dataset in geodata:
    try:
        print(f"Processing {dataset['name']}...")
        
        # Build the full path to the shapefile
        filepath = os.path.join(base_path, dataset["name"], f"{dataset['name']}.shp")
        
        # Read the shapefile into a GeoDataFrame
        gdf = read_shapefile(filepath)
        
        # Ensure text fields are correctly encoded and stripped of unwanted characters
        for column in gdf.columns:
            if gdf[column].dtype == object:
                gdf[column] = gdf[column].astype(str).str.encode('utf-8', 'ignore').str.decode('utf-8').str.strip()
        
        # Convert polygon geometries to point (centroid) for specific datasets
        if dataset["name"] in polygon_to_point:
            gdf["geometry"] = gdf["geometry"].centroid
        
        # Convert the GeoDataFrame to GeoJSON
        gdf_json = json.loads(gdf.to_json())
        
        # Transform data to remove 'Feature' type and flatten properties
        transformed_data = []
        for feature in gdf_json["features"]:
            new_entry = {
                "type": dataset["type"],
                "location": {
                    "longitude": feature["geometry"]["coordinates"][0],
                    "latitude": feature["geometry"]["coordinates"][1]
                },
                "geometry_type": "point",
            }
            
            # Remove area_id from coverage and FID from islands
            properties = feature["properties"]
            if dataset["name"] == "spatial_coverage":
                properties.pop("area_id", None)
            if dataset["name"] == "islands":
                properties.pop("FID", None)
            
            # Remove Lat and Lon fields if they exist
            properties.pop("Lat", None)
            properties.pop("Lon", None)
            
            new_entry.update(properties)
            transformed_data.append(new_entry)
        
        # Insert transformed data into the unified collection
        collection.insert_many(transformed_data)
        
        print(f"{dataset['name']} successfully imported into unified collection: geodata!")
    except Exception as e:
        print(f"Error processing {dataset['name']}: {e}")

# Create a 2dsphere index on the location field
collection.create_index([("location", "2dsphere")])

print("All geospatial data have been imported into a single collection: geodata!")