import os
import pandas as pd
from pymongo import MongoClient, ASCENDING
from datetime import datetime

# MongoDB Connection
client = MongoClient("mongodb://localhost:27017/")
db = client["maritime"]  
collection = db["dynamic_vessels"]

# Load Static Data to Get Country Info
static_collection = db["vessels"]  
static_data = {doc["_id"]: doc.get("country", "Unknown") for doc in static_collection.find({}, {"_id": 1, "country": 1})}

# Data Directory
data_dir = "C:\\Users\\DIAMAT\\Desktop\\MongoDB\\Data\\Extracted\\unipi_ais_dynamic_2019"

# Process CSV Files in Chunks
for file in os.listdir(data_dir):
    if file.endswith(".csv"):
        file_path = os.path.join(data_dir, file)
        print(f"Processing {file}...")

        batch_size = 10000  # Adjust this for memory efficiency
        dynamic_documents = []

        for chunk in pd.read_csv(file_path, chunksize=batch_size):
            for _, row in chunk.iterrows():
                vessel_id = row["vessel_id"]
                vessel_country = static_data.get(vessel_id, "Unknown")

                dynamic_doc = {
                    "timestamp": datetime.utcfromtimestamp(row["t"] / 1000),
                    "vessel": {"vessel_id": vessel_id, "country": vessel_country},
                    "location": {"longitude": row["lon"], "latitude": row["lat"]},
                    "heading": row["heading"],
                    "speed": row["speed"],
                    "course": row["course"]
                }
                dynamic_documents.append(dynamic_doc)

            # Bulk insert every batch_size records
            if dynamic_documents:
                collection.insert_many(dynamic_documents, ordered=False, bypass_document_validation=True)
                print(f"Inserted {len(dynamic_documents)} records from {file}.")
                dynamic_documents = []  # Clear memory

print("Data import completed.")
