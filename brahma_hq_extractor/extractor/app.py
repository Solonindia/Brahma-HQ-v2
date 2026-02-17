from fastapi import FastAPI, Request
from extractor.main import handle_event

app = FastAPI()

@app.get("/")
def health():
    return {"status": "ok"}

@app.post("/")
async def eventarc_receiver(request: Request):
    event = await request.json()
    data = event.get("data", {})
    print("📩 EVENT RECEIVED:", event.get("type"), data.get("name"))
    handle_event(event)
    return {"status": "ok"}
