from fastapi import FastAPI

app = FastAPI()

@app.get("/")
async def read_root():
    return {"message": "Data Engineering Challenge server is running!"}

@app.get("/status")
async def get_status():
    return {"status": "Ingesting data..."}