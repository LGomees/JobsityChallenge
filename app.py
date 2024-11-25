import subprocess
from fastapi import FastAPI, File, UploadFile
import os

app = FastAPI()

# Ensure the directory exists
os.makedirs("data/", exist_ok=True)

@app.get("/")
async def read_root():
    return {"message": "Data Engineering Challenge server is running!"}

@app.get("/status")
async def get_status():
    return {"status": "Ingesting data..."}

@app.post("/run-ingestion")
async def run_ingestion(file: UploadFile = File(...)):
    try:
        # Define the path where the file will be saved
        file_path = os.path.join("data/", file.filename)
        
        # Save the uploaded CSV file to the specified directory
        with open(file_path, "wb") as f:
            contents = await file.read()
            f.write(contents)
        
        # Run your ingestion script with the file path as an argument
        result = subprocess.run(
            ["python3", "etl/etl.py", file_path], 
            capture_output=True,
            text=True,
            check=True
        )

        # Return success message with the output from the script
        return {"message": "Data ingestion and store was successful!", "output": result.stdout}
    
    except subprocess.CalledProcessError as e:
        # Handle errors during script execution
        return {"message": "Error during data ingestion execution", "error": e.stderr}
    except Exception as e:
        # Handle other exceptions
        return {"message": "An error occurred", "error": str(e)}
