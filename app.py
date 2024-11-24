import subprocess
from fastapi import FastAPI

app = FastAPI()

@app.get("/")
async def read_root():
    return {"message": "Data Engineering Challenge server is running!"}

@app.get("/status")
async def get_status():
    return {"status": "Ingesting data..."}

@app.get("/run-ingestion")
async def run_ingestion():
    try:
        result = subprocess.run(
            ["python", "etl/ingest.py"],  
            capture_output=True,      
            text=True,                
            check=True                
        )
        return {"message": "Script executed successfuly", "output": result.stdout}
    except subprocess.CalledProcessError as e:
        return {"message": "Error during script execution", "error": e.stderr}