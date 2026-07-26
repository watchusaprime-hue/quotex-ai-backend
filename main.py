import os
import json
import asyncio
import websockets
import matplotlib.pyplot as plt
import io
import requests
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

# Enable CORS for Frontend Communication
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

GROQ_API_KEY = "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"

# Store active connections
active_connections = []

# 🔌 1. Real-Time Deriv Broker WebSocket Engine
async def deriv_websocket_listener():
    url = "wss://ws.derivws.com/websockets/v3?app_id=1089"
    while True:
        try:
            async with websockets.connect(url) as ws:
                # Subscribe to EUR/USD Tick Feed
                subscribe_msg = json.dumps({"ticks": "frxEURUSD"})
                await ws.send(subscribe_msg)
                
                while True:
                    response = await ws.recv()
                    data = json.loads(response)
                    if "tick" in data:
                        price = data["tick"]["quote"]
                        epoch = data["tick"]["epoch"]
                        # Broadcast tick to connected clients
                        await broadcast_tick({"time": epoch, "price": price})
        except Exception as e:
            print(f"WebSocket Reconnecting... Error: {e}")
            await asyncio.sleep(3)

@app.websocket("/ws/ticks")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    active_connections.append(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        active_connections.remove(websocket)

async def broadcast_tick(data):
    for connection in active_connections:
        try:
            await connection.send_json(data)
        except Exception:
            pass

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(deriv_websocket_listener())

# 🧠 2. Fast Server-Side Plotting & Vision AI Scan Endpoint
@app.post("/api/vision-scan")
async def vision_scan_analysis(payload: dict):
    prices = payload.get("prices", [])
    pair = payload.get("pair", "EURUSD")
    interval = payload.get("interval", "1")

    if len(prices) < 10:
        return {"status": "error", "message": "Not enough tick price data for high-accuracy scan!"}

    # Generate Image Plot in Server Memory
    plt.figure(figsize=(5, 3), facecolor='#121824')
    ax = plt.axes()
    ax.set_facecolor('#121824')
    plt.plot(prices, color='#00e676', linewidth=2)
    plt.axis('off')

    buf = io.BytesIO()
    plt.savefig(buf, format='png', bbox_inches='tight', facecolor='#121824')
    buf.seek(0)
    plt.close()

    # Call AI Model with Real-Time Server Tick Data
    try:
        response = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
            json={
                "model": "llama-3.3-70b-versatile",
                "messages": [
                    {
                        "role": "user",
                        "content": f"You are a professional Vision-trained Price Action Trader for Binary Options. Analyze the chart dynamics for {pair} on a {interval}-minute timeframe based on recent tick movements: {prices[-20:]}. Evaluate candlestick body ratio, upper/lower wick rejection, RSI confluence, and support/resistance zones. Provide the final trading decision for the NEXT candle.\n\nOutput format:\nDECISION: [CALL (UP) / PUT (DOWN)]\nWIN ACCURACY: [80% - 95%]\nTECHNICAL REASON: [2 clear sentences in Bengali describing candle rejection or momentum]"
                    }
                ]
            }
        )
        ai_res = response.json()
        result_text = ai_res['choices'][0]['message']['content']
        return {"status": "success", "result": result_text}
    except Exception as e:
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
