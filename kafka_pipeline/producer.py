"""
Kafka Producer for ATM Withdrawal Transactions
Streams continuous withdrawal transactions for ATMs to the Kafka topic.
Simulates realistic cash balances, customer withdrawals, and cash-outs.
"""

import time
import random
import threading
from datetime import datetime
from typing import Dict, Any, List
from kafka_pipeline.kafka_manager import kafka_manager, KAFKA_TOPIC
from data.generate_dataset import ATM_PROFILES, CARD_TYPES, DENOMINATIONS

# Active state tracking for ATMs in memory
_ATM_STATE = {
    atm["atm_id"]: {
        "atm_id": atm["atm_id"],
        "name": atm["name"],
        "traffic_type": atm["traffic_type"],
        "location": atm["location"],
        "max_capacity": atm["max_capacity"],
        "current_cash": atm["current_cash"],
        "total_dispensed_today": 0,
        "txns_today": 0,
        "last_replenished": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    for atm in ATM_PROFILES
}

_state_lock = threading.Lock()


class ATMKafkaProducer:
    """Streams ATM withdrawal events to Kafka."""
    def __init__(self):
        self.is_running = False
        self._thread = None
        self.interval = 1.5
        self.txn_counter = 500000

    def generate_single_event(self) -> Dict[str, Any]:
        """Generate one real-time ATM withdrawal transaction."""
        with _state_lock:
            # Pick ATM with probability weighted by traffic type
            weights = [3 if a["traffic_type"] == "High-Traffic" else (2 if a["traffic_type"] == "Medium-Traffic" else 1) for a in ATM_PROFILES]
            atm_prof = random.choices(ATM_PROFILES, weights=weights, k=1)[0]
            atm_id = atm_prof["atm_id"]
            atm_state = _ATM_STATE[atm_id]

            self.txn_counter += 1
            amount = random.choice(DENOMINATIONS)

            # Check if ATM has enough cash
            if atm_state["current_cash"] >= amount:
                atm_state["current_cash"] -= amount
                atm_state["total_dispensed_today"] += amount
                atm_state["txns_today"] += 1
                status = "SUCCESS"
            else:
                status = "CASH_OUT_FAILED"  # ATM ran out of cash! (Stockout)

            txn_event = {
                "transaction_id": f"TXN_STREAM_{self.txn_counter}",
                "atm_id": atm_id,
                "atm_name": atm_state["name"],
                "location": atm_state["location"],
                "traffic_type": atm_state["traffic_type"],
                "amount_withdrawn": amount if status == "SUCCESS" else 0,
                "requested_amount": amount,
                "card_type": random.choice(CARD_TYPES),
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "remaining_cash": atm_state["current_cash"],
                "transaction_status": status,
                "alert": "🚨 STOCKOUT OCCURRED!" if status == "CASH_OUT_FAILED" else None
            }

        return txn_event

    def _stream_loop(self):
        while self.is_running:
            event = self.generate_single_event()
            kafka_manager.send_message(KAFKA_TOPIC, event)
            time.sleep(self.interval)

    def start_streaming(self, interval: float = 1.5):
        if not self.is_running:
            self.is_running = True
            self.interval = interval
            self._thread = threading.Thread(target=self._stream_loop, daemon=True)
            self._thread.start()
            print(f"[Kafka Producer] Started streaming ATM transactions to topic '{KAFKA_TOPIC}' (Interval: {interval}s)")

    def stop_streaming(self):
        self.is_running = False
        if self._thread:
            self._thread.join(timeout=2)
        print("[Kafka Producer] Stopped streaming ATM transactions.")

    @staticmethod
    def replenish_atm(atm_id: str, amount: float) -> Dict[str, Any]:
        """Replenish cash into an ATM."""
        with _state_lock:
            if atm_id in _ATM_STATE:
                _ATM_STATE[atm_id]["current_cash"] = min(
                    _ATM_STATE[atm_id]["max_capacity"],
                    _ATM_STATE[atm_id]["current_cash"] + amount
                )
                _ATM_STATE[atm_id]["last_replenished"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                return _ATM_STATE[atm_id]
        return {}

    @staticmethod
    def get_all_atm_states() -> List[Dict[str, Any]]:
        with _state_lock:
            return [dict(v) for v in _ATM_STATE.values()]


# Global producer instance
atm_producer = ATMKafkaProducer()

if __name__ == "__main__":
    print("Testing single Kafka event production:")
    event = atm_producer.generate_single_event()
    print(kafka_manager.send_message(KAFKA_TOPIC, event))
