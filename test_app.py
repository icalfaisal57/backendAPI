from fastapi import FastAPI
import os

app = FastAPI()

@app.get("/")
def root():
    return {
        "status": "online",
        "port": os.getenv("PORT", "not set"),
        "database_url": "configured" if os.getenv("DATABASE_URL") else "not configured"
    }

@app.get("/health")
def health():
    return {"status": "healthy"}

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
```

Update `Procfile`:
```
web: uvicorn test_app:app --host 0.0.0.0 --port $PORT
