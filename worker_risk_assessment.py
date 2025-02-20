from flask import Flask
import pandas as pd
import numpy as np
from pymongo import MongoClient
from pymongo.errors import BulkWriteError
from flask_cors import CORS
 

app = Flask(__name__)
CORS(app)

MONGO_URI = "mongodb+srv://suraaksha1:6fm727LtiPZiYy9I@democluster.1hibj.mongodb.net/suraaksha?retryWrites=true&w=majority"
DB_NAME = "suraaksha"
COLLECTION_WORKFORCES = "workforces"
COLLECTION_WORKPLACES = "workplaces"
COLLECTION_ASSIGNMENTS = "worker_assignments"

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
        score += 50
    return score

def calculate_safe_hours(worker, workplace):
    safe_hours = 8
    if workplace["CO2 Levels (ppm)"] > 400:
        safe_hours -= (workplace["CO2 Levels (ppm)"] - 400) / 100
    if workplace["Ambient Temperature (C)"] > 30:
        safe_hours -= (workplace["Ambient Temperature (C)"] - 30) / 2
    if workplace["Elevation (m)"] > 500:
        safe_hours -= (workplace["Elevation (m)"] - 500) / 100 * 0.5
    if workplace["Hazard Levels"] > 5:
        safe_hours -= (workplace["Hazard Levels"] - 5) * 0.5
    return max(safe_hours, 0)

@app.route('/')
def assign_workers():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    workforces_collection = db[COLLECTION_WORKFORCES]
    workplaces_collection = db[COLLECTION_WORKPLACES]
    assignments_collection = db[COLLECTION_ASSIGNMENTS]

    workforces = pd.DataFrame(list(workforces_collection.find({}, {'_id': 0})))
    workplaces = pd.DataFrame(list(workplaces_collection.find({}, {'_id': 0})))

    N = 20
    floor_capacity = {floor: 0 for floor in workplaces["Floor_ID"]}

    assignments = []
    workforces["Risk_Score"] = workforces.apply(lambda worker: min(
        calculate_risk(worker, workplace) for _, workplace in workplaces.iterrows()
    ), axis=1)

    sorted_workers = workforces.sort_values(by="Risk_Score")

    for _, worker in sorted_workers.iterrows():
        best_workplace = None
        best_score = float('inf')
        safe_hours = 0

        for _, workplace in workplaces.iterrows():
            temp_safe_hours = calculate_safe_hours(worker, workplace)
            if temp_safe_hours > 0 and floor_capacity[workplace["Floor_ID"]] < N:
                risk = calculate_risk(worker, workplace)
                if risk < best_score:
                    best_score = risk
                    best_workplace = workplace
                    safe_hours = temp_safe_hours

        if best_workplace is not None:
            risk_level = "High" if best_score > 120 else "Moderate" if best_score > 80 else "Safe"
            suggested_move = "No"
            if risk_level == "High":
                for _, alternative_wp in workplaces.iterrows():
                    if alternative_wp["Hazard Levels"] < best_workplace["Hazard Levels"] and calculate_safe_hours(worker, alternative_wp) > 0:
                        suggested_move = f"Move to {alternative_wp['Floor_ID']}"
                        break

            assignments.append({
                "EMP_CODE": worker["EMP_CODE"],
                "Assigned_Floor": best_workplace["Floor_ID"],
                "Safe_Work_Duration_Hours": safe_hours,
                "Risk_Level": risk_level,
                "Suggested_Move": suggested_move
            })
            floor_capacity[best_workplace["Floor_ID"]] += 1
        else:
            assignments.append({
                "EMP_CODE": worker["EMP_CODE"],
                "Assigned_Floor": "Unassigned",
                "Safe_Work_Duration_Hours": 0,
                "Risk_Level": "No Safe Floor",
                "Suggested_Move": "Find Alternative"
            })

    assignments_collection.delete_many({})
    try:
        assignments_collection.insert_many(assignments, ordered=False)
    except BulkWriteError:
        pass

    return "Worker assignments stored in database."

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)