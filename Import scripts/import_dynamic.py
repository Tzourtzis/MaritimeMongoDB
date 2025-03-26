import os
import pandas as pd
from pymongo import MongoClient
from datetime import datetime

# MongoDB Connection
client = MongoClient("mongodb://localhost:27017/")
db = client["maritime"]  # Database name
collection = db["dynamic_vessels"]  # Collection name

# Clear the collection before inserting new data
# collection.delete_many({})

# Load Static Data to Get Country Info
static_collection = db["vessels"]  # Existing static vessel data collection
static_data = {doc["_id"]: doc.get("country", "Unknown") for doc in static_collection.find({}, {"_id": 1, "country": 1})}

# Directory containing CSV files
data_dir = "C:\\Users\\DIAMAT\\Desktop\\MongoDB\\Data\\Extracted\\unipi_ais_dynamic_2018"



# Process each CSV file in the directory
for file in os.listdir(data_dir):
    if file.endswith(".csv"):
        file_path = os.path.join(data_dir, file)
        print(f"Processing {file}...")

        # Load the CSV data
        dynamic_df = pd.read_csv(file_path)

        # Process and insert data
        dynamic_documents = []
        for _, row in dynamic_df.iterrows():
            vessel_id = row["vessel_id"]
            vessel_country = static_data.get(vessel_id, "Unknown")

            dynamic_doc = {
                "timestamp": datetime.utcfromtimestamp(row["timestamp"] / 1000),  # Convert UNIX timestamp to ISODate
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
                "course": row["course"]
            }
            dynamic_documents.append(dynamic_doc)

        # Insert into MongoDB
        if dynamic_documents:
            collection.insert_many(dynamic_documents, ordered=False, bypass_document_validation=True)
            print(f"Inserted {len(dynamic_documents)} records from {file} into MongoDB.")
        else:
            print(f"No data to insert from {file}.")

print("Data import completed for the available CSV files.")
