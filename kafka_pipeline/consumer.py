"""
Kafka Consumer for ATM Transactions
Subscribes to Kafka topic, buffers streamed transactions,
and writes them to Hadoop HDFS partitioned storage.
"""

import time
import threading
from typing import Dict, Any, List, Callable
from kafka_pipeline.kafka_manager import kafka_manager, KAFKA_TOPIC
from hdfs_storage.hdfs_client import hdfs_client


class ATMKafkaConsumer:
    """Consumes Kafka ATM transactions and persists them to HDFS."""
    def __init__(self, batch_size: int = 10, flush_interval: float = 5.0):
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.buffer: List[Dict[str, Any]] = []
        self.lock = threading.Lock()
        self.is_running = False
        self._flush_thread = None
        self.total_consumed = 0
        self.total_hdfs_batches = 0
        self.live_subscribers: List[Callable[[Dict[str, Any]], None]] = []

        # Register callback with kafka manager
        kafka_manager.subscribe(KAFKA_TOPIC, self._on_message_received)

    def _on_message_received(self, kafka_record: Dict[str, Any]):
        """Handler called when a message is received from Kafka."""
        txn_data = kafka_record.get("value", {})
        with self.lock:
            self.buffer.append(txn_data)
            self.total_consumed += 1

            # Notify UI live subscribers
            for sub in list(self.live_subscribers):
                try:
                    sub(txn_data)
                except Exception:
                    pass

            if len(self.buffer) >= self.batch_size:
                self._flush_to_hdfs()

    def _flush_to_hdfs(self):
        """Flushes buffered transactions to HDFS partition."""
        if not self.buffer:
            return

        to_flush = list(self.buffer)
        self.buffer.clear()
        
        try:
            hdfs_path = hdfs_client.write_transaction_batch(to_flush)
            self.total_hdfs_batches += 1
            # print(f"[Kafka Consumer] Persisted {len(to_flush)} txns to HDFS -> {hdfs_path}")
        except Exception as e:
            print(f"[Kafka Consumer] Error persisting to HDFS: {e}")

    def _periodic_flusher(self):
        while self.is_running:
            time.sleep(self.flush_interval)
            with self.lock:
                if self.buffer:
                    self._flush_to_hdfs()

    def start(self):
        if not self.is_running:
            self.is_running = True
            self._flush_thread = threading.Thread(target=self._periodic_flusher, daemon=True)
            self._flush_thread.start()
            print("[Kafka Consumer] Consumer initialized and listening for Kafka stream.")

    def stop(self):
        self.is_running = False
        with self.lock:
            self._flush_to_hdfs()

    def register_ui_subscriber(self, callback: Callable[[Dict[str, Any]], None]):
        """Attach dashboard WebSocket/UI listener."""
        self.live_subscribers.append(callback)

    def unregister_ui_subscriber(self, callback: Callable[[Dict[str, Any]], None]):
        if callback in self.live_subscribers:
            self.live_subscribers.remove(callback)

    def get_consumer_status(self) -> Dict[str, Any]:
        with self.lock:
            return {
                "status": "Active / Consuming" if self.is_running else "Standby",
                "total_consumed_messages": self.total_consumed,
                "buffered_in_memory": len(self.buffer),
                "total_batches_sent_to_hdfs": self.total_hdfs_batches,
                "batch_flush_threshold": self.batch_size
            }


# Global consumer instance
atm_consumer = ATMKafkaConsumer()

if __name__ == "__main__":
    atm_consumer.start()
    print("Consumer Status:", atm_consumer.get_consumer_status())
