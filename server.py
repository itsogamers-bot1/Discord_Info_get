from threading import Thread

from fastapi import FastAPI
import uvicorn

app = FastAPI()

@app.get("/")
async def root():
	return {"message": "Server is Online."}

# added for uptimerobot
@app.head("/")
async def head():
    return {}  # No body needed for HEAD, only headers will be returned

def start():
	uvicorn.run(app, host="0.0.0.0", port=8080)

def server_thread():
	t = Thread(target=start)
	t.start()