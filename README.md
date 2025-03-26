# 🚢 Maritime Data Modeling with MongoDB

This repository contains the deliverables for a graduate-level project focused on the design and implementation of a non-relational database in **MongoDB**. The project centers around managing large-scale maritime data, including ship trajectories, ports, islands, and weather conditions in the Piraeus region.

---

## 📁 Project Structure

### 1. Technical Report
- `Technical_Report.pdf`: A detailed report covering data modeling, index design, data import processes, and performance evaluation in MongoDB. Includes analysis of spatial and spatiotemporal queries.

### 2. `data_import/` Folder
Includes all Python scripts used to transform and import datasets into MongoDB:

- `import_ais_static.py`: Imports static ship data (e.g., identifiers, characteristics).
- `import_dynamic.py`: Imports dynamic ship position data.
- `import_dynamic - improved.py`: Optimized version of dynamic data import.
- `import_geospatial.py`: Imports geographical data (e.g., ports, islands).
- `import_synopses.py`: Imports summarized vessel trajectory data.
- `import_weather.py`: Imports meteorological records.

---

## ⚙️ Getting Started

### Requirements

- MongoDB installed and running.
- [MongoDB Compass](https://www.mongodb.com/try/download/compass) (optional, for GUI-based inspection).
- Python environment with `pandas`, `pymongo`, and `pyshp` installed.

### Data Import

From the `data_import/` directory, run:

```bash
python import_ais_static.py
python import_dynamic - improved.py
python import_geospatial.py
python import_synopses.py
python import_weather.py
```

Data is loaded into a MongoDB database named `maritime`.

### Creating Indexes

To speed up queries, especially spatial and spatiotemporal, create the following indexes:

```javascript
db.dynamic_vessels.createIndex({ "location": "2dsphere" })
db.geodata.createIndex({ "region": 1 })
```

---

## 🧪 Experimentation & Evaluation

The database was tested for:

- **Relational queries** (e.g., vessel lookup by country or name).
- **Spatial queries** (e.g., vessels within a radius from a location).
- **Spatiotemporal queries** (e.g., vessels near a location during a specific period).

Experiments demonstrated that **compound indexes** on spatial and temporal fields greatly reduce query times.

---

## 💡 Key Features

- Designed collections based on entity relationships (1:1, 1:N, N:M).
- Used embedding selectively to optimize performance.
- Benchmarked spatial/spatiotemporal query execution time.
- Evaluated the scalability of the database by comparing performance across different data volumes (2017–2019).
- Explored sharding, although full deployment was limited by local technical constraints.

---

## 👨‍💻 Authors

- Angelos Tzourtzis – [aggelos.tzurtzis@gmail.com](mailto:aggelos.tzurtzis@gmail.com)  
- Dimitrios Galatidis – [dimitriosgalatidis@yahoo.com](mailto:dimitriosgalatidis@yahoo.com)

📍 MSc Students, University of Piraeus  
🗓️ Submitted: February 2025

---

Feel free to fork this repo, run the scripts, and explore maritime data at scale 🌍📦
