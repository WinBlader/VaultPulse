"""
Next-Day Cash Demand Predictor & ATM Replenishment Engine
Loads trained time-series model, predicts next-day cash demand per ATM,
and calculates optimal cash replenishment recommendations to minimize stockout & excess cash.
"""

import os
import sys
import json
import joblib
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from forecasting.train_model import MODEL_FILE, METRICS_FILE, load_dataset, create_time_series_features, FEATURE_COLUMNS
from kafka_pipeline.producer import atm_producer
from data.generate_dataset import is_public_holiday


class ReplenishmentOptimizer:
    """Predicts next-day demand and recommends cash replenishment per ATM."""
    def __init__(self):
        self.model_data = None
        self.metrics = None
        self._load_artifacts()

    def _load_artifacts(self):
        if os.path.exists(MODEL_FILE):
            try:
                self.model_data = joblib.load(MODEL_FILE)
            except Exception as e:
                print(f"[Replenishment Engine] Error loading model: {e}")

        if os.path.exists(METRICS_FILE):
            try:
                with open(METRICS_FILE, "r") as f:
                    self.metrics = json.load(f)
            except Exception:
                self.metrics = None

    def ensure_trained(self):
        if self.model_data is None or self.metrics is None:
            from forecasting.train_model import train_forecasting_model
            self.metrics = train_forecasting_model()
            self._load_artifacts()

    def predict_next_day_demand(self) -> list:
        """
        Generate predictions for tomorrow's cash demand across all ATMs
        and determine optimal replenishment amounts.
        """
        self.ensure_trained()
        model = self.model_data["model"]
        feature_names = self.model_data["features"]
        atm_columns = self.model_data["atm_columns"]

        df_history = load_dataset()
        df_featured = create_time_series_features(df_history)

        atm_states = {a["atm_id"]: a for a in atm_producer.get_all_atm_states()}
        
        tomorrow = datetime.now() + timedelta(days=1)
        tomorrow_str = tomorrow.strftime("%Y-%m-%d")
        day_of_week = tomorrow.strftime("%A")
        is_weekend = 1 if tomorrow.weekday() >= 5 else 0
        is_holiday = 1 if is_public_holiday(tomorrow) else 0

        recommendations = []

        for atm_id, atm_state in atm_states.items():
            # Get latest historical data for this ATM to build tomorrow's lag features
            atm_hist = df_featured[df_featured["atm_id"] == atm_id].sort_values("date")
            if atm_hist.empty:
                continue

            last_row = atm_hist.iloc[-1]
            lag_1 = float(last_row["total_demand"])
            lag_2 = float(last_row["lag_1"])
            
            # rolling 7-day mean
            recent_demands = atm_hist["total_demand"].tail(7).tolist()
            rolling_mean_7 = float(np.mean(recent_demands))
            rolling_std_7 = float(np.std(recent_demands)) if len(recent_demands) > 1 else 1000.0
            lag_7 = float(atm_hist["total_demand"].iloc[-7]) if len(atm_hist) >= 7 else rolling_mean_7

            feature_dict = {
                "lag_1": lag_1,
                "lag_2": lag_2,
                "lag_7": lag_7,
                "rolling_mean_7": rolling_mean_7,
                "rolling_std_7": rolling_std_7,
                "day_of_week_num": tomorrow.weekday(),
                "is_weekend": is_weekend,
                "is_holiday": is_holiday,
                "day_of_month": tomorrow.day
            }

            # One-hot ATM indicator
            for col in atm_columns:
                feature_dict[col] = 1 if col == f"atm_{atm_id}" else 0

            X_input = pd.DataFrame([feature_dict])[feature_names]
            pred_demand = float(model.predict(X_input)[0])
            pred_demand = max(1000.0, round(pred_demand, -2))  # Round to hundreds

            # Safety Stock Buffer (15% above forecast to absorb surges)
            safety_buffer = round(pred_demand * 0.15, -2)
            target_cash_capacity = pred_demand + safety_buffer

            current_cash = float(atm_state["current_cash"])
            max_capacity = float(atm_state["max_capacity"])

            # Recommended replenishment cash
            if current_cash < target_cash_capacity:
                replenishment_amount = min(max_capacity - current_cash, round(target_cash_capacity - current_cash, -2))
                replenishment_amount = max(0.0, replenishment_amount)
            else:
                replenishment_amount = 0.0

            # Classification of Risk (Stockout vs Excess Cash vs Optimal)
            if current_cash < pred_demand:
                risk_status = "STOCKOUT_ALERT"
                risk_label = "🚨 Stockout Risk (Cash Out Imminent)"
                risk_color = "danger"
                reason = f"Current cash (${current_cash:,.0f}) is BELOW tomorrow's predicted demand (${pred_demand:,.0f}). Machine will run dry!"
            elif current_cash > (target_cash_capacity * 2.2):
                risk_status = "EXCESS_CASH_ALERT"
                risk_label = "⚠️ Excess Cash Risk (Idle Capital)"
                risk_color = "warning"
                reason = f"Current cash (${current_cash:,.0f}) is over 2.2x needed. ${current_cash - target_cash_capacity:,.0f} idle cash should be swept."
            else:
                risk_status = "OPTIMAL"
                risk_label = "🟢 Optimal Cash Level"
                risk_color = "success"
                reason = "Current cash balance comfortably meets tomorrow's predicted demand with safety buffer."

            # Calculate comparison with dumb fixed schedule ($35,000 flat)
            fixed_schedule_amount = 35000.0
            fixed_stockout = pred_demand > (current_cash + fixed_schedule_amount)
            fixed_excess = (current_cash + fixed_schedule_amount) > (target_cash_capacity * 2.5)

            recommendations.append({
                "atm_id": atm_id,
                "atm_name": atm_state["name"],
                "traffic_type": atm_state["traffic_type"],
                "location": atm_state["location"],
                "tomorrow_date": tomorrow_str,
                "day_of_week": day_of_week,
                "is_weekend": bool(is_weekend),
                "is_holiday": bool(is_holiday),
                "current_cash": current_cash,
                "max_capacity": max_capacity,
                "predicted_demand": pred_demand,
                "safety_stock": safety_buffer,
                "target_cash_level": target_cash_capacity,
                "recommended_replenishment": replenishment_amount,
                "risk_status": risk_status,
                "risk_label": risk_label,
                "risk_color": risk_color,
                "operational_reason": reason,
                "fixed_schedule_flaw": "Would Stockout" if fixed_stockout else ("Idle Waste" if fixed_excess else "Passable")
            })

        return recommendations

    def get_historical_and_forecast_chart_data(self, atm_id: str = "ATM_001", days_back: int = 14) -> dict:
        """Returns actual history and forecast for Chart.js visualization."""
        self.ensure_trained()
        df_history = load_dataset()
        atm_df = df_history[df_history["atm_id"] == atm_id].sort_values("date").tail(days_back).copy()

        labels = [pd.to_datetime(d).strftime("%b %d") for d in atm_df["date"]]
        actual_demands = atm_df["total_demand"].tolist()

        # Add tomorrow forecast
        recs = {r["atm_id"]: r for r in self.predict_next_day_demand()}
        tomorrow_rec = recs.get(atm_id, {})
        
        forecast_labels = list(labels)
        forecast_labels.append("Tomorrow (Forecast)")

        # In Chart.js: actual history line has null on tomorrow; forecast line connects last actual to tomorrow forecast
        actual_series = list(actual_demands) + [None]
        forecast_series = [None] * (len(actual_demands) - 1) + [actual_demands[-1], tomorrow_rec.get("predicted_demand", 0)]

        return {
            "atm_id": atm_id,
            "atm_name": tomorrow_rec.get("atm_name", atm_id),
            "labels": forecast_labels,
            "actual_demands": actual_series,
            "forecast_demands": forecast_series,
            "tomorrow_prediction": tomorrow_rec.get("predicted_demand", 0),
            "current_cash": tomorrow_rec.get("current_cash", 0)
        }


# Global optimizer instance
replenishment_optimizer = ReplenishmentOptimizer()

if __name__ == "__main__":
    recs = replenishment_optimizer.predict_next_day_demand()
    print("Next-Day Replenishment Recommendations:")
    for r in recs:
        status_clean = r['risk_status']
        print(f"[{r['atm_id']}] {r['atm_name']} ({r['traffic_type']}): Current Cash: ${r['current_cash']:,.0f} | Tomorrow Forecast: ${r['predicted_demand']:,.0f} | Load: ${r['recommended_replenishment']:,.0f} | Status: {status_clean}")
