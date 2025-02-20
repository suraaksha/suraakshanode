import pandas as pd
import chardet  # Auto-detect encoding
import numpy as np
import os
from tabulate import tabulate  # For better table formatting
from pymongo import MongoClient  # MongoDB connection

# MongoDB connection details
MONGO_URI = "mongodb+srv://suraaksha1:6fm727LtiPZiYy9I@democluster.1hibj.mongodb.net/suraaksha?retryWrites=true&w=majority" # Change if using a remote DB
DB_NAME = "suraaksha"  # Replace with your actual DB name
COLLECTION_NAME = "worker_assignments"


# Define local file paths
base_dir = os.path.dirname(os.path.abspath(__file__))  # Get the script's directory
workers_file = os.path.join(base_dir, "workers.csv")
workplaces_file = os.path.join(base_dir, "workplaces.csv")

# Detect encoding dynamically
def detect_encoding(file_path):
    with open(file_path, "rb") as f:
        result = chardet.detect(f.read(100000))  # Read first 100KB
    return result["encoding"]

workers_encoding = detect_encoding(workers_file)
workplaces_encoding = detect_encoding(workplaces_file)

# Load CSV files with correct separator
workers = pd.read_csv(workers_file, encoding=workers_encoding, sep=",")
workplaces = pd.read_csv(workplaces_file, encoding=workplaces_encoding, sep=",")

# Fix column names if tab characters were retained in headers
workers.columns = workers.columns[0].split(",") if len(workers.columns) == 1 else workers.columns
workplaces.columns = workplaces.columns[0].split(",") if len(workplaces.columns) == 1 else workplaces.columns

# Define risk assessment function
def calculate_risk(worker, workplace):
    score = 0
    if worker["Heart Rate (BPM)"] > 100:
        score += workplace["CO2 Levels (ppm)"] * 2
    if worker["Mean Arterial Pressure (mmHg)"] > 90:
        score += workplace["Elevation (m)"] * 1.5
    if worker["Respiratory Rate (breaths/min)"] > 20:
        score += workplace["Humidity (%)"] * 1.2
    if worker["Heart Rate Variability (ms)"] < 50:
        score += workplace["Vibration Exposure (Hz)"] * 2
    if workplace["Hazard Levels"] > 7:
        score += 50  # High hazard penalty
    return score

# Define safe work duration calculation
def calculate_safe_hours(worker, workplace):
    safe_hours = 8  # Default shift duration

    if workplace["CO2 Levels (ppm)"] > 400:
        safe_hours -= (workplace["CO2 Levels (ppm)"] - 400) / 100
    if workplace["Ambient Temperature (C)"] > 30:
        safe_hours -= (workplace["Ambient Temperature (C)"] - 30) / 2
    if workplace["Elevation (m)"] > 500:
        safe_hours -= (workplace["Elevation (m)"] - 500) / 100 * 0.5
    if workplace["Hazard Levels"] > 5:
        safe_hours -= (workplace["Hazard Levels"] - 5) * 0.5

    return max(safe_hours, 0)  # Ensure hours don't go negative

# Set a limit on the number of workers per floor
N = 20  # Maximum workers allowed per floor
print("Columns in workplaces:", workplaces.columns.tolist())

floor_capacity = {floor: 0 for floor in workplaces["Floor_ID"]}  # Track floor assignments

# Assign workers to best workplace & assess risk
assignments = []

# Sort workers by increasing risk score for optimal allocation
workers["Risk_Score"] = workers.apply(lambda worker: min(
    calculate_risk(worker, workplace) for _, workplace in workplaces.iterrows()
), axis=1)

sorted_workers = workers.sort_values(by="Risk_Score")  # Lower risk workers get assigned first

for _, worker in sorted_workers.iterrows():
    best_workplace = None
    best_score = float('inf')
    safe_hours = 0

    # Find the best available floor that isn't full and has safe work hours > 0
    for _, workplace in workplaces.iterrows():
        temp_safe_hours = calculate_safe_hours(worker, workplace)
        if temp_safe_hours > 0 and floor_capacity[workplace["Floor_ID"]] < N:  # Check capacity & safety
            risk = calculate_risk(worker, workplace)
            if risk < best_score:
                best_score = risk
                best_workplace = workplace
                safe_hours = temp_safe_hours

    if best_workplace is not None:
        # Determine Risk Level
        if best_score > 120:
            risk_level = "High"
        elif best_score > 80:
            risk_level = "Moderate"
        else:
            risk_level = "Safe"

        # Suggest moving worker to an easier workplace if risk is high
        suggested_move = "No"
        if risk_level == "High":
            for _, alternative_wp in workplaces.iterrows():
                if alternative_wp["Hazard Levels"] < best_workplace["Hazard Levels"] and calculate_safe_hours(worker, alternative_wp) > 0:
                    suggested_move = f"Move to {alternative_wp['Floor_ID']}"
                    break

        # Assign worker and update floor capacity
        assignments.append([worker["EMP_CODE"], best_workplace["Floor_ID"], safe_hours, risk_level, suggested_move])
        floor_capacity[best_workplace["Floor_ID"]] += 1  # Increase count
    else:
        # If no safe floor was found, mark as unassigned
        assignments.append([worker["EMP_CODE"], "Unassigned", 0, "No Safe Floor", "Find Alternative"])

# Save results to CSV
output_file = os.path.join(base_dir, "worker_assignments.csv")
assignments_df = pd.DataFrame(assignments, columns=["EMP_CODE", "Assigned_Floor", "Safe_Work_Duration_Hours", "Risk_Level", "Suggested_Move"])
assignments_df.to_csv(output_file, index=False)

print(f" Worker assignments saved to: {output_file}")

# Define the saved file path
output_file = os.path.join(base_dir, "worker_assignments.csv")

# Read the worker assignments CSV
try:
    assignments_df = pd.read_csv(output_file)

    # Display the data in table format
    print("\n Worker Assignments Table:\n")
    print(tabulate(assignments_df, headers="keys", tablefmt="pretty"))

except FileNotFoundError:
    print(f"Error: The file '{output_file}' was not found. Ensure the previous script ran successfully.")
except Exception as e:
    print(f"An error occurred: {e}")


# Connect to MongoDB
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

# Ensure EMP_CODE is unique by creating a unique index
collection.create_index([("EMP_CODE", 1)], unique=True)

# Convert DataFrame to a list of dictionaries
assignments_data = assignments_df.to_dict(orient="records")
from pymongo.errors import BulkWriteError

try:
    collection.insert_many(assignments_data, ordered=False)  # ordered=False allows inserting valid records
    print(f"Data successfully stored in MongoDB collection: {COLLECTION_NAME}")
except BulkWriteError as e:
    print("Duplicate EMP_CODE entries found. Skipping duplicates...")


print(f"Data successfully stored in MongoDB collection: {COLLECTION_NAME}")
