import os
import pandas as pd
from pymongo import MongoClient
from datetime import datetime

# MongoDB Connection
client = MongoClient("mongodb://localhost:27017/")
db = client["maritime"]  # Database name
collection = db["synopses"]  # Collection name

# Load Static Data to Get Country Info
static_collection = db["vessels"]  # Existing static vessel data collection
static_data = {doc["_id"]: doc.get("country", "Unknown") for doc in static_collection.find({}, {"_id": 1, "country": 1})}

# Directory containing CSV files
data_dir = "C:\\Users\\DIAMAT\\Desktop\\MongoDB\\Data\\Extracted\\unipi_ais_dynamic_synopses\\ais_synopses\\2019"

# List of months to process
months = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"]

# Process each CSV file for the specified months
for month in months:
    file_name = f"unipi_ais_synopses_{month}_2019.csv"
    file_path = os.path.join(data_dir, file_name)
    
    if os.path.exists(file_path):
        print(f"Processing {file_name}...")
        
        # Load the CSV data
        dynamic_df = pd.read_csv(file_path)

        # Process and insert data
        dynamic_documents = []
        for _, row in dynamic_df.iterrows():
            vessel_id = row["vessel_id"]
            vessel_country = static_data.get(vessel_id, "Unknown")
            
            dynamic_doc = {
                "timestamp": datetime.utcfromtimestamp(row["t"] / 1000),  # Convert UNIX timestamp to ISODate
                "vessel": {
                    "vessel_id": vessel_id,
                    "country": vessel_country
                },
                "location": {
                    "longitude": row["lon"],
                    "latitude": row["lat"]
                },
                "heading": row["heading"],
                "speed": row["speed"],
                "annotations": row["annotations"],
                "transport_trail": row["transport_trail"]
            }
            dynamic_documents.append(dynamic_doc)

        # Insert into MongoDB
        if dynamic_documents:
            collection.insert_many(dynamic_documents, ordered=False, bypass_document_validation=True)
            print(f"Inserted {len(dynamic_documents)} records from {file_name} into MongoDB.")
        else:
            print(f"No data to insert from {file_name}.")
    else:
        print(f"File {file_name} not found, skipping...")

print("Data import completed for selected months.")
