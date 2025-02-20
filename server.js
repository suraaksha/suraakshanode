const express = require("express");
const mongoose = require("mongoose");
const bodyParser = require("body-parser");
const { spawn } = require("child_process");
const path = require("path");
const app = express();
const PORT = process.env.PORT || 2000;

// Middleware

const cors = require("cors");
const { exec } = require('child_process');
const fs = require("fs");
app.use(cors({ origin: 'https://suraakshaweb.vercel.app' }));
app.use(bodyParser.json());

// MongoDB Connection
mongoose
  .connect(
    "mongodb+srv://suraaksha1:6fm727LtiPZiYy9I@democluster.1hibj.mongodb.net/suraaksha?retryWrites=true&w=majority",
    {
      useUnifiedTopology: true,
    }
    
  )
  .then(() => console.log("MongoDB connected"))
  .catch((err) => console.error("Error connecting to MongoDB:", err));

// Define Workforce Schema
const WorkforceSchema = new mongoose.Schema({
  EMP_CODE: { type: String, required: true, unique: true },
  Floor_ID: String,
  "Heart Rate (BPM)": Number,
  "Cardiac Output (L/min)": Number,
  "Mean Arterial Pressure (mmHg)": Number,
  "Heart Rate Variability (ms)": Number,
  "Systolic BP (mmHg)": Number,
  "Diastolic BP (mmHg)": Number,
  "Respiratory Rate (breaths/min)": Number,
  LX: Number,
  LY: Number,
});

const Workforce = mongoose.model("Workforce", WorkforceSchema);

// Define WorkPlace Schema
const WorkPlaceSchema = new mongoose.Schema({
  Floor_ID: String,
  "CO2 Levels (ppm)": Number,
  "Ambient Temperature (C)": Number,
  "Humidity (%)": Number,
  "Elevation (m)": Number,
  "Vibration Exposure (Hz)": Number,
  "Hazard Levels": Number,
});

const WorkPlace = mongoose.model("WorkPlace", WorkPlaceSchema);

// Define Mongoose Schema for workers_assignments
const WorkerSchema = new mongoose.Schema({
  EMP_CODE: { type: String, unique: true },
  Assigned_Floor: String,
  Safe_Work_Duration_Hours: Number,
  Risk_Level: String,
  Suggested_Move: String,
});

const Worker = mongoose.model("worker_assignments", WorkerSchema);

// API to fetch all workers from workers_assignments collection
app.get("/api/workers-assignment", async (req, res) => {
  try {
    const workers = await Worker.find(); // Fetch all workers
    res.json(workers);
  } catch (error) {
    res.status(500).json({ message: "Error fetching workers", error });
  }
});

app.post("/api/uploadWorkforce", async (req, res) => {
  try {
    const workforceData = req.body;

    // Check if the incoming data is an array
    if (!Array.isArray(workforceData)) {
      console.error("Error: workforceData is not an array");
      return res.status(400).json({ error: "Invalid data format" });
    }

    // Iterate through each workforce data item and update or insert it
    for (const workforce of workforceData) {
      await Workforce.updateOne(
        { EMP_CODE: workforce.EMP_CODE }, // Use EMP_CODE as the unique identifier
        {
          $set: {
            Floor_ID: workforce.Floor_ID,
            "Heart Rate (BPM)": workforce["Heart Rate (BPM)"],
            "Cardiac Output (L/min)": workforce["Cardiac Output (L/min)"],
            "Mean Arterial Pressure (mmHg)": workforce["Mean Arterial Pressure (mmHg)"],
            "Heart Rate Variability (ms)": workforce["Heart Rate Variability (ms)"],
            "Systolic BP (mmHg)": workforce["Systolic BP (mmHg)"],
            "Diastolic BP (mmHg)": workforce["Diastolic BP (mmHg)"],
            "Respiratory Rate (breaths/min)": workforce["Respiratory Rate (breaths/min)"],
            LX: workforce.LX,
            LY: workforce.LY,
          },
        },
        { upsert: true }
      );
    }

    // Send success response after processing all the data
    res.status(201).json({ message: "Workforce data uploaded successfully!" });
  } catch (err) {
    console.error("Error saving workforce data:", err);
    res.status(500).json({ message: "Server error while uploading workforce data" });
  }
});




// API Endpoint for WorkPlace Upload
app.post("/api/uploadWorkPlace", async (req, res) => {
  try {
    const workPlaceData = req.body;

    if (!Array.isArray(workPlaceData)) {
      console.error("Error: workPlaceData is not an array");
      return res.status(400).json({ error: "Invalid data format" });
    }

    for (const place of workPlaceData) {
      await WorkPlace.updateOne(
        { Floor_ID: place.Floor_ID }, // Use Floor_ID as the unique identifier
        {
          $set: {
            "CO2 Levels (ppm)": place["CO2 Levels (ppm)"],
            "Ambient Temperature (C)": place["Ambient Temperature (C)"],
            "Humidity (%)": place["Humidity (%)"],
            "Elevation (m)": place["Elevation (m)"],
            "Vibration Exposure (Hz)": place["Vibration Exposure (Hz)"],
            "Hazard Levels": place["Hazard Levels"],
          },
        },
        { upsert: true }
      );
    }

    res.status(201).json({ message: "Workplace data uploaded successfully!" });
  } catch (err) {
    console.error("Error saving workplace data:", err);
    res.status(500).json({ message: "Server error while uploading workplace data" });
  }
});


const EmployeeSchema = new mongoose.Schema({
  personalDetails: {
    name: String,
    employeeId: String,
    age: Number,
    gender: String,
  },
  occupationalDetails: {
    department: String,
    jobTitle: String,
    workShift: String,
    workLocation: String,
    yearsOfService: Number,
  },
  physicalHealth: {
    height: Number,
    weight: Number,
    bmi: Number,
    bloodPressure: String,
    heartRate: Number,
    bloodOxygen: Number,
    bodyTemperature: Number,
    respirationRate: Number,
    ecg: String,
    hrv: Number,
  },
  emergencyPreparedness: {
    firstAidTraining: String,
    emergencyContact: String,
    medicalConditions: [String],
    allergies: [String],
  },
  recommendations: [String],
})

  
const  Employee= mongoose.model("Employee", EmployeeSchema, "DetailsOfEmployee");

/* fs.readFile("workers_data (2).json", "utf8", (err, data) => {
  if (err) {
    console.error("Error reading JSON file:", err);
    return;
  }

  const workers = JSON.parse(data);
  Employee.insertMany(workers)
    .then(() => {
      console.log("Data successfully uploaded!");
      mongoose.connection.close();
    })
    .catch((err) => console.error("Error inserting data:", err));
});*/

app.get("/api/emp", async (req, res) => {
  try {
    const employees = await Employee.find({}); // Fetch all fields
    res.json(employees);
  } catch (error) {
    res.status(500).json({ message: "Error fetching workers", error });
  }
});


app.get("/api/emp/:id", async (req, res) => {
  try {
    const employee = await Employee.findOne({ "_id": req.params.id }); // Query by employeeId
    if (!employee) {
      return res.status(404).json({ message: "Worker not found" });
    }
    res.json(employee);
  } catch (error) {
    console.error("Error fetching worker data:", error);
    res.status(500).json({ message: "Error fetching worker data" });
  }
});



const DayWorkerSchema = new mongoose.Schema({
  Date: {
    type: String, // Use string format if you prefer the date to be stored as a string
    required: true,
  },
  EMP_CODE: {
    type: String,
    required: true,
  },
  Assigned_Floor: {
    type: String,
    required: true,
  },
  Safe_Work_Duration_Hours: {
    type: Number,
    required: true,
  },
  Risk_Level: {
    type: String,
    required: true,
  },
  Suggested_Move: {
    type: String,
    required: true,
  }
}, { timestamps: true });
  
const DayWorker = mongoose.model("DaywiseWorker", DayWorkerSchema, "Day_wise_worker_assignments");

/* fs.readFile("employee_data_updated.json", "utf8", (err, data) => {
  if (err) {
    console.error("Error reading JSON file:", err);
    return;
  }

  const workers = JSON.parse(data);
  DayWorker.insertMany(workers)
    .then(() => {
      console.log("Data successfully uploaded!");
      mongoose.connection.close();
    })
    .catch((err) => console.error("Error inserting data:", err));
}); */  


const employeeLocationSchema = new mongoose.Schema({
  EMP_CODE: {
      type: String,
      required: true
  },
  Assigned_Floor: {
      type: String,
      required: true
  },
  X: {
      type: Number,
      required: true
  },
  Y: {
      type: Number,
      required: true
  }
});

const EmployeeLocation = mongoose.model('EmployeeLocation', employeeLocationSchema);

module.exports = EmployeeLocation;
/*
 fs.readFile("employee_xy_data (1).json", "utf8", (err, data) => {
  if (err) {
    console.error("Error reading JSON file:", err);
    return;
  }

  const workersLocation = JSON.parse(data);
  EmployeeLocation.insertMany(workersLocation)
    .then(() => {
      console.log("Data successfully uploaded!");
      mongoose.connection.close();
    })
    .catch((err) => console.error("Error inserting data:", err));
}); */

app.get('/api/employee/:empCode', async (req, res) => {
  const { empCode } = req.params;  // Get EMP_CODE from the URL

  try {
    // Find all employees in the database matching the EMP_CODE
    const employees = await EmployeeLocation.find({ EMP_CODE: empCode });

    // If no employees are found, return a 404 error
    if (employees.length === 0) {
      return res.status(404).json({ message: "No employees found for the given EMP_CODE" });
    }

    // Return all matching employee data as a JSON response
    res.json(employees);
  } catch (error) {
    // If there's an error, return a 500 status code and the error message
    console.error(error);
    res.status(500).json({ message: "Server error", error });
  }
});


app.get('/api/workerlocation', async (req, res) => {
  try {
    // Retrieve unique EMP_CODE from the collection
    const workerloc = await EmployeeLocation.find();
    res.json(workerloc);
  } catch (err) {
    console.error('Error fetching workers:', err);
    res.status(500).send('Internal Server Error');
  }
});



app.get('/api/workerscode', async (req, res) => {
  try {
    // Retrieve unique EMP_CODE from the collection
    const workerscode = await DayWorker.distinct('EMP_CODE');
    res.json(workerscode);
  } catch (err) {
    console.error('Error fetching workers:', err);
    res.status(500).send('Internal Server Error');
  }
});

app.get('/api/workers/:workerCode', async (req, res) => {
  const { workerCode } = req.params;
  try { 
    const workerData = await DayWorker.find({ EMP_CODE: workerCode });

    if (!workerData || workerData.length === 0) {
      return res.status(404).json({ error: 'Worker data not found for this Employee code' });
    }

    res.status(200).json(workerData);
  } catch (error) {
    console.error("Error fetching worker details:", error);
    res.status(500).json({ error: 'Internal server error', details: error.message });
  }
});



app.get('/api/workers/:workerCode/:date', async (req, res) => {
  const { workerCode, date } = req.params;

  // Ensure that the date is in the right format, i.e., "YYYY-MM-DD"
  const formattedDate = date; // No need to parse it if it's already in the "YYYY-MM-DD" format

  try { 
    const worker = await DayWorker.findOne({
      EMP_CODE: workerCode,
      Date: formattedDate,  // Compare using the string format directly
    });

    if (!worker) {
      return res.status(404).json({ error: 'Worker data not found for this date' });
    }

    res.status(200).json(worker);
  } catch (error) {
    console.error("Error fetching worker details:", error);
    res.status(500).json({ error: 'Internal server error' });
  }
});
// Check what type your Date field is stored as







// Start the Server
app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
