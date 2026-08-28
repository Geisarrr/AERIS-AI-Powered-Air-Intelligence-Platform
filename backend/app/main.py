from fastapi import FastAPI

app = FastAPI(
    title="AERIS API",
    description="AI-Powered Air Intelligence Platform",
    version="0.1.0",
)


@app.get("/")
def root():
    return {
        "message": "AERIS API is running",
        "version": "0.1.0",
    }


@app.get("/health")
def health_check():
    return {
        "status": "healthy",
    }