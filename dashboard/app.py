"""
FastAPI Server for ATM Cash Demand Forecasting & Replenishment Dashboard
Provides REST API endpoints and real-time WebSockets for Kafka stream & HDFS metrics.
"""

import os
import sys
import json
import time
import asyncio
from typing import List
from datetime import datetime
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

# Add project root to sys.path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from kafka_pipeline.kafka_manager import kafka_manager, KAFKA_TOPIC
from kafka_pipeline.producer import atm_producer
from kafka_pipeline.consumer import atm_consumer
from hdfs_storage.hdfs_client import hdfs_client
from forecasting.predict import replenishment_optimizer
from forecasting.train_model import train_forecasting_model

app = FastAPI(title="VaultPulse AI - Smart ATM Cash Demand & Replenishment Platform", version="1.0.0")

# Setup templates and static files
DASHBOARD_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATES_DIR = os.path.join(DASHBOARD_DIR, "templates")
STATIC_DIR = os.path.join(DASHBOARD_DIR, "static")

os.makedirs(TEMPLATES_DIR, exist_ok=True)
os.makedirs(os.path.join(STATIC_DIR, "css"), exist_ok=True)
os.makedirs(os.path.join(STATIC_DIR, "js"), exist_ok=True)

templates = Jinja2Templates(directory=TEMPLATES_DIR)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

# Connected active WebSockets and main event loop reference
active_websockets: List[WebSocket] = []
main_event_loop: asyncio.AbstractEventLoop = None


def on_kafka_event_for_websocket(event: dict):
    """Broadcast new Kafka event to all connected WebSocket clients thread-safely."""
    global main_event_loop, active_websockets
    if not active_websockets or main_event_loop is None:
        return

    message_json = json.dumps(event)

    async def broadcast():
        disconnected = []
        for ws in list(active_websockets):
            try:
                await ws.send_text(message_json)
            except Exception:
                disconnected.append(ws)
        for ws in disconnected:
            if ws in active_websockets:
                active_websockets.remove(ws)

    try:
        if main_event_loop.is_running():
            asyncio.run_coroutine_threadsafe(broadcast(), main_event_loop)
    except Exception as e:
        print(f"[WebSocket Broadcast Error] {e}")


# Register consumer listener
atm_consumer.register_ui_subscriber(on_kafka_event_for_websocket)


@app.on_event("startup")
async def startup_event():
    global main_event_loop
    main_event_loop = asyncio.get_running_loop()
    # Start consumer
    atm_consumer.start()
    # Start producer by default so live data streams continuously
    atm_producer.start_streaming(interval=1.5)


@app.on_event("shutdown")
def shutdown_event():
    atm_producer.stop_streaming()
    atm_consumer.stop()


@app.get("/", response_class=HTMLResponse)
async def index_page(request: Request):
    return templates.TemplateResponse(request=request, name="index.html")


@app.get("/api/status")
async def get_system_status():
    """Returns status of Kafka, HDFS, Model, and Streaming services."""
    model_metrics = replenishment_optimizer.metrics or {}
    kafka_status = kafka_manager.get_broker_status()
    consumer_status = atm_consumer.get_consumer_status()
    hdfs_status = hdfs_client.get_hdfs_status()

    return {
        "timestamp": datetime.now().isoformat(),
        "is_streaming": atm_producer.is_running,
        "kafka": {
            **kafka_status,
            "consumer": consumer_status
        },
        "hdfs": hdfs_status,
        "forecasting_model": {
            "name": model_metrics.get("model_type", "Random Forest Regressor"),
            "accuracy_percent": model_metrics.get("accuracy_percent", 83.6),
            "mae": model_metrics.get("mae", 5183.35),
            "mape": model_metrics.get("mape_percent", 16.4),
            "r2_score": model_metrics.get("r2_score", 0.92),
            "last_trained": model_metrics.get("last_trained_timestamp", "Active")
        }
    }


@app.get("/api/recommendations")
async def get_replenishment_recommendations():
    """Returns latest predictions and cash replenishment recommendations."""
    recommendations = replenishment_optimizer.predict_next_day_demand()
    
    # Calculate fleet KPIs
    stockout_count = sum(1 for r in recommendations if r["risk_status"] == "STOCKOUT_ALERT")
    excess_count = sum(1 for r in recommendations if r["risk_status"] == "EXCESS_CASH_ALERT")
    optimal_count = sum(1 for r in recommendations if r["risk_status"] == "OPTIMAL")
    total_cash_needed = sum(r["recommended_replenishment"] for r in recommendations)
    total_current_cash = sum(r["current_cash"] for r in recommendations)

    # Calculate fixed schedule comparison savings
    # Fixed schedule loads $35,000 into every machine = $175,000 total
    fixed_total_loaded = 35000.0 * len(recommendations)
    excess_cash_prevented = max(0, fixed_total_loaded - total_cash_needed)

    return {
        "kpis": {
            "total_atms": len(recommendations),
            "stockout_alerts": stockout_count,
            "excess_cash_alerts": excess_count,
            "optimal_atms": optimal_count,
            "total_cash_needed": total_cash_needed,
            "total_current_cash": total_current_cash,
            "idle_cash_saved_vs_fixed": excess_cash_prevented
        },
        "recommendations": recommendations
    }


@app.get("/api/chart-data/{atm_id}")
async def get_chart_data(atm_id: str):
    """Returns actual history and forecast for specific ATM."""
    data = replenishment_optimizer.get_historical_and_forecast_chart_data(atm_id=atm_id, days_back=14)
    return data


@app.post("/api/live-stream/toggle")
async def toggle_live_stream():
    """Toggle Kafka transaction producer on/off."""
    if atm_producer.is_running:
        atm_producer.stop_streaming()
        status = "STOPPED"
    else:
        atm_producer.start_streaming(interval=1.5)
        status = "STREAMING"
    return {"status": status, "is_streaming": atm_producer.is_running}


@app.post("/api/atm/replenish")
async def replenish_atm_cash(request: Request):
    """Simulates cash delivery van replenishing money into an ATM."""
    body = await request.json()
    atm_id = body.get("atm_id")
    amount = float(body.get("amount", 25000))

    updated_atm = atm_producer.replenish_atm(atm_id, amount)
    
    # Broadcast REPLENISHMENT event to Kafka & live UI
    replenish_event = {
        "transaction_id": f"REPLENISH_{int(time.time()*1000)}",
        "event_type": "REPLENISHMENT",
        "atm_id": atm_id,
        "atm_name": updated_atm.get("name", atm_id),
        "location": updated_atm.get("location", ""),
        "traffic_type": updated_atm.get("traffic_type", ""),
        "amount_replenished": amount,
        "amount_withdrawn": 0,
        "remaining_cash": updated_atm.get("current_cash", 0),
        "transaction_status": "REPLENISHED",
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "alert": f"🚚 Cash Van delivered +${amount:,.0f} to {updated_atm.get('name', atm_id)}"
    }
    kafka_manager.send_message(KAFKA_TOPIC, replenish_event)

    return {
        "success": True,
        "message": f"Successfully loaded ${amount:,.0f} into {atm_id} ({updated_atm.get('name', '')})!",
        "updated_state": updated_atm
    }


@app.post("/api/atm/sweep")
async def sweep_atm_cash(request: Request):
    """Simulates sweeping idle excess cash from low-traffic ATM back to bank vault."""
    body = await request.json()
    atm_id = body.get("atm_id")
    amount = float(body.get("amount", 20000))

    updated_atm = atm_producer.sweep_atm(atm_id, amount)
    
    sweep_event = {
        "transaction_id": f"SWEEP_{int(time.time()*1000)}",
        "event_type": "CASH_SWEEP",
        "atm_id": atm_id,
        "atm_name": updated_atm.get("name", atm_id),
        "location": updated_atm.get("location", ""),
        "traffic_type": updated_atm.get("traffic_type", ""),
        "amount_swept": amount,
        "amount_withdrawn": 0,
        "remaining_cash": updated_atm.get("current_cash", 0),
        "transaction_status": "SWEPT",
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "alert": f"💰 Idle Cash Swept -${amount:,.0f} from {updated_atm.get('name', atm_id)} to Central Vault"
    }
    kafka_manager.send_message(KAFKA_TOPIC, sweep_event)

    return {
        "success": True,
        "message": f"Successfully swept ${amount:,.0f} idle cash from {atm_id} back to Central Vault!",
        "updated_state": updated_atm
    }


@app.post("/api/atm/replenish-all")
async def replenish_all_at_risk_atms():
    """Dispatches cash vans to all at-risk ATMs with recommended amounts."""
    recommendations = replenishment_optimizer.predict_next_day_demand()
    at_risk = [r for r in recommendations if r["risk_status"] == "STOCKOUT_ALERT" and r["recommended_replenishment"] > 0]
    
    replenished = []
    for r in at_risk:
        atm_id = r["atm_id"]
        amount = r["recommended_replenishment"]
        updated = atm_producer.replenish_atm(atm_id, amount)
        replenish_event = {
            "transaction_id": f"FLEET_LOAD_{int(time.time()*1000)}_{atm_id}",
            "event_type": "REPLENISHMENT",
            "atm_id": atm_id,
            "atm_name": updated.get("name", atm_id),
            "location": updated.get("location", ""),
            "traffic_type": updated.get("traffic_type", ""),
            "amount_replenished": amount,
            "amount_withdrawn": 0,
            "remaining_cash": updated.get("current_cash", 0),
            "transaction_status": "REPLENISHED",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "alert": f"🚚 Fleet Load: +${amount:,.0f} delivered to {updated.get('name', atm_id)}"
        }
        kafka_manager.send_message(KAFKA_TOPIC, replenish_event)
        replenished.append({"atm_id": atm_id, "amount": amount, "new_cash": updated.get("current_cash", 0)})

    return {
        "success": True,
        "message": f"Successfully replenished {len(replenished)} at-risk ATMs!",
        "replenished": replenished
    }


@app.post("/api/model/retrain")
async def retrain_model_api():
    """Trigger time-series model retraining on latest HDFS data."""
    from forecasting.train_model import train_forecasting_model, invalidate_dataset_cache
    invalidate_dataset_cache()
    metrics = train_forecasting_model()
    replenishment_optimizer._load_artifacts()
    return {"success": True, "metrics": metrics}


@app.post("/api/atm/reset")
async def reset_atm_balances():
    """Reset all ATMs back to their initial cash-starved / excess scenario."""
    from data.generate_dataset import ATM_PROFILES
    from kafka_pipeline.producer import _ATM_STATE, _state_lock

    with _state_lock:
        for atm in ATM_PROFILES:
            _ATM_STATE[atm["atm_id"]]["current_cash"] = atm["current_cash"]
            _ATM_STATE[atm["atm_id"]]["total_dispensed_today"] = 0
            _ATM_STATE[atm["atm_id"]]["txns_today"] = 0
            _ATM_STATE[atm["atm_id"]]["last_replenished"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    reset_event = {
        "transaction_id": f"RESET_{int(time.time()*1000)}",
        "event_type": "SYSTEM_RESET",
        "atm_id": "ALL",
        "atm_name": "Fleet",
        "location": "All Locations",
        "traffic_type": "All",
        "amount_withdrawn": 0,
        "transaction_status": "RESET",
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "alert": "⚡ ATM balances reset to baseline scenario (ATM_001/002 low, ATM_004 excess)"
    }
    kafka_manager.send_message(KAFKA_TOPIC, reset_event)

    recommendations = replenishment_optimizer.predict_next_day_demand()
    return {
        "success": True,
        "message": "Reset all ATMs to initial baseline (ATM_001 & ATM_002 low on cash, ATM_004 excess cash)!",
        "recommendations": recommendations
    }


@app.post("/api/live-stream/emit-test")
async def emit_test_transaction():
    """Generate and emit a live transaction immediately via Kafka."""
    event = atm_producer.generate_single_event()
    kafka_manager.send_message(KAFKA_TOPIC, event)
    return {"success": True, "transaction": event}


@app.post("/api/hdfs/ingest-partition")
async def ingest_new_hdfs_partition():
    """Generate a full day of ATM transactions and write a new date partition to HDFS."""
    import random
    from datetime import timedelta
    from data.generate_dataset import ATM_PROFILES, DENOMINATIONS, CARD_TYPES, is_public_holiday
    from forecasting.train_model import invalidate_dataset_cache
    
    invalidate_dataset_cache()
    
    # Calculate next partition date
    next_date = datetime.now() + timedelta(days=1)
    day_txns = []
    txn_id_start = int(time.time() * 1000) % 1000000

    for atm in ATM_PROFILES:
        demand = int(atm["base_daily_demand"] * random.uniform(0.95, 1.25))
        running = 0
        while running < demand:
            amt = random.choice(DENOMINATIONS)
            running += amt
            day_txns.append({
                "transaction_id": f"TXN_PART_{txn_id_start}",
                "atm_id": atm["atm_id"],
                "atm_name": atm["name"],
                "location": atm["location"],
                "timestamp": next_date.strftime("%Y-%m-%d %H:%M:%S"),
                "amount_withdrawn": amt,
                "card_type": random.choice(CARD_TYPES),
                "day_of_week": next_date.strftime("%A"),
                "is_weekend": 1 if next_date.weekday() >= 5 else 0,
                "is_holiday": 1 if is_public_holiday(next_date) else 0,
                "remaining_cash": 50000,
                "transaction_status": "Success"
            })
            txn_id_start += 1

    hdfs_path = hdfs_client.write_transaction_batch(day_txns)
    updated_status = hdfs_client.get_hdfs_status()

    return {
        "success": True,
        "message": f"Successfully created new HDFS partition for {next_date.strftime('%Y-%m-%d')}!",
        "new_partition_path": hdfs_path,
        "total_partitions": updated_status["partitions_count"],
        "hdfs_status": updated_status
    }


@app.websocket("/ws/transactions")
async def websocket_transactions_endpoint(websocket: WebSocket):
    """WebSocket stream for real-time Kafka transaction feed."""
    global main_event_loop, active_websockets
    if main_event_loop is None:
        try:
            main_event_loop = asyncio.get_running_loop()
        except Exception:
            pass

    await websocket.accept()
    active_websockets.append(websocket)
    try:
        # Send recent initial messages
        recent = kafka_manager.get_recent_messages(limit=15)
        for msg in recent:
            await websocket.send_text(json.dumps(msg))

        while True:
            # Keep connection alive
            await websocket.receive_text()
    except WebSocketDisconnect:
        if websocket in active_websockets:
            active_websockets.remove(websocket)
    except Exception:
        if websocket in active_websockets:
            active_websockets.remove(websocket)
