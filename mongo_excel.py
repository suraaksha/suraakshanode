import pymongo
import pandas as pd
import os

# MongoDB Atlas Connection String (Replace with your credentials)
MONGO_URI = "mongodb+srv://suraaksha1:6fm727LtiPZiYy9I@democluster.1hibj.mongodb.net/suraaksha?retryWrites=true&w=majority"

# Connect to MongoDB Atlas
client = pymongo.MongoClient(MONGO_URI)
db = client["suraaksha"]
collection_workforces = db["workforces"]
collection_workplaces = db["workplaces"]

# Fetch data (Exclude '_id' field for better CSV formatting)
data_wf = list(collection_workforces.find({}, {"_id": 0}))
data_wp = list(collection_workplaces.find({}, {"_id": 0}))

# Convert to Pandas DataFrame
df1 = pd.DataFrame(data_wf)
df2 = pd.DataFrame(data_wp)

# **Dynamic Target Directory** - Same directory as the script
target_dir = os.path.dirname(os.path.abspath(__file__))

# Save files in the same directory
csv_filename1 = os.path.join(target_dir, "workers.csv")
csv_filename2 = os.path.join(target_dir, "workplaces.csv")

df1.to_csv(csv_filename1, index=False)
df2.to_csv(csv_filename2, index=False)

print(f"Data exported successfully to {csv_filename1}")
print(f"Data exported successfully to {csv_filename2}")
