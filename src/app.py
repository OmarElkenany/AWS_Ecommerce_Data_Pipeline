from fastapi import FastAPI

app = FastAPI()


@app.get("/")
def home():
    return {
        "message": "E-commerce Ordering API is running"
    }


@app.get("/health")
def health():
    return {
        "status": "OK"
    }