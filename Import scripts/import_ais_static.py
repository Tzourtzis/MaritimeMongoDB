import pandas as pd
from pymongo import MongoClient

# MongoDB Connection
client = MongoClient("mongodb://localhost:27017/")
db = client["maritime"]  # Database name
collection = db["vessels"]  # Collection name

# Clear the collection before inserting new data
collection.delete_many({})

# Load CSV files
codes_df = pd.read_csv("C:/Users/DIAMAT/Desktop/MongoDB/Data/Extracted/ais_static/ais_codes_descriptions.csv")
vessels_df = pd.read_csv("C:/Users/DIAMAT/Desktop/MongoDB/Data/Extracted/ais_static/unipi_ais_static.csv")

# Convert shiptype codes to descriptions
codes_dict = dict(zip(codes_df["Type Code"], codes_df["Description"]))

# Process and insert data
vessel_documents = []
for _, row in vessels_df.iterrows():
    vessel_id = row["vessel_id"]
    if vessel_id in [doc["_id"] for doc in vessel_documents]:
        continue  # Skip duplicates
    
    vessel_doc = {
        "_id": vessel_id,  # Set vessel_id as MongoDB _id
        "country": row["country"] if pd.notna(row["country"]) else None,
        "shiptype": {
            "code": row["shiptype"] if pd.notna(row["shiptype"]) else None,
            "description": codes_dict.get(int(row["shiptype"])) if pd.notna(row["shiptype"]) else "Unknown"
        }
    }
    vessel_documents.append(vessel_doc)

# Insert into MongoDB
if vessel_documents:
    try:
        collection.insert_many(vessel_documents, ordered=False)
        print(f"Inserted {len(vessel_documents)} vessel documents into MongoDB.")
    except Exception as e:
        print(f"Error inserting documents: {e}")
else:
    print("No data to insert.")
