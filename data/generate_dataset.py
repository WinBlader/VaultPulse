"""
Synthetic ATM Transaction Dataset Generator
Inspired by Kaggle ATM withdrawal datasets and retail banking transaction logs.
Generates realistic multi-ATM transaction streams with seasonality, weekday/weekend effects, and holiday spikes.
"""

import os
import random
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

# Configure ATM profiles
ATM_PROFILES = [
    {
        "atm_id": "ATM_001",
        "name": "Metro Central Station",
        "traffic_type": "High-Traffic",
        "location": "Downtown Transit Hub",
        "base_daily_demand": 55000,
        "demand_std": 6000,
        "weekend_multiplier": 1.25,
        "holiday_multiplier": 1.50,
        "max_capacity": 100000,
        "current_cash": 18000 # Prone to stockout
    },
    {
        "atm_id": "ATM_002",
        "name": "Grand City Mall",
        "traffic_type": "High-Traffic",
        "location": "Shopping Center",
        "base_daily_demand": 48000,
        "demand_std": 5000,
        "weekend_multiplier": 1.60,
        "holiday_multiplier": 1.70,
        "max_capacity": 90000,
        "current_cash": 12000 # High stockout risk
    },
    {
        "atm_id": "ATM_003",
        "name": "Financial Tower Plaza",
        "traffic_type": "Medium-Traffic",
        "location": "Business District",
        "base_daily_demand": 32000,
        "demand_std": 4000,
        "weekend_multiplier": 0.45, # Drops on weekends
        "holiday_multiplier": 0.30,
        "max_capacity": 70000,
        "current_cash": 35000 # Healthy
    },
    {
        "atm_id": "ATM_004",
        "name": "Suburban Green Park",
        "traffic_type": "Low-Traffic",
        "location": "Residential Community",
        "base_daily_demand": 8500,
        "demand_std": 1500,
        "weekend_multiplier": 1.10,
        "holiday_multiplier": 1.20,
        "max_capacity": 50000,
        "current_cash": 45000 # Excess idle cash risk
    },
    {
        "atm_id": "ATM_005",
        "name": "International Airport T2",
        "traffic_type": "High-Traffic",
        "location": "Airport Arrivals Gate",
        "base_daily_demand": 62000,
        "demand_std": 7000,
        "weekend_multiplier": 1.15,
        "holiday_multiplier": 1.40,
        "max_capacity": 120000,
        "current_cash": 70000 # Healthy
    }
]

CARD_TYPES = ["Visa", "MasterCard", "RuPay", "American Express"]
DENOMINATIONS = [20, 40, 50, 100, 200, 300, 400, 500]


def is_public_holiday(date_obj: datetime) -> bool:
    """Simulate realistic public and banking holidays."""
    # Specific holidays or 1st/15th of the month spikes (salary days)
    if date_obj.day in [1, 2, 28, 29, 30, 31]:
        return True
    if date_obj.month in [1, 7, 10, 11, 12] and date_obj.day in [1, 4, 15, 25]:
        return True
    return False


def generate_synthetic_data(num_days: int = 120, output_dir: str = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Generate synthetic historical ATM transactions and daily demand summaries.
    """
    if output_dir is None:
        output_dir = os.path.dirname(os.path.abspath(__file__))
    os.makedirs(output_dir, exist_ok=True)

    print(f"Generating synthetic ATM transactions for {len(ATM_PROFILES)} ATMs over {num_days} days...")
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=num_days)

    transactions = []
    daily_summaries = []
    txn_id_counter = 100001

    for day_offset in range(num_days):
        current_date = start_date + timedelta(days=day_offset)
        day_of_week = current_date.strftime("%A")
        is_weekend = 1 if current_date.weekday() >= 5 else 0
        is_holiday = 1 if is_public_holiday(current_date) else 0

        for atm in ATM_PROFILES:
            # Calculate target daily demand based on profile, seasonality, and calendar effects
            demand = atm["base_daily_demand"]
            if is_weekend:
                demand *= atm["weekend_multiplier"]
            if is_holiday:
                demand *= atm["holiday_multiplier"]

            # Add random standard deviation noise
            noise = np.random.normal(0, atm["demand_std"])
            daily_total = max(2000, int(demand + noise))

            # Simulate individual transactions across the 24 hours of the day
            running_day_cash = 0
            atm_cash_balance = atm["max_capacity"]

            # Generate individual withdrawal transactions that sum up approximately to daily_total
            daily_txn_count = 0
            while running_day_cash < daily_total:
                amount = random.choice(DENOMINATIONS)
                running_day_cash += amount
                daily_txn_count += 1
                
                # Random time during day (peak hours: 10am-2pm, 5pm-9pm)
                peak_factor = random.random()
                if peak_factor > 0.4:
                    hour = random.randint(10, 20)
                else:
                    hour = random.randint(0, 23)
                
                minute = random.randint(0, 59)
                second = random.randint(0, 59)
                txn_time = current_date.replace(hour=hour, minute=minute, second=second)

                atm_cash_balance = max(0, atm_cash_balance - amount)
                status = "Success" if atm_cash_balance > 0 else "Cash_Out_Failed"

                transactions.append({
                    "transaction_id": f"TXN_{txn_id_counter}",
                    "atm_id": atm["atm_id"],
                    "atm_name": atm["name"],
                    "location": atm["location"],
                    "timestamp": txn_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "amount_withdrawn": amount,
                    "card_type": random.choice(CARD_TYPES),
                    "day_of_week": day_of_week,
                    "is_weekend": is_weekend,
                    "is_holiday": is_holiday,
                    "remaining_cash": atm_cash_balance,
                    "transaction_status": status
                })
                txn_id_counter += 1

            # Daily aggregate record
            daily_summaries.append({
                "date": current_date.strftime("%Y-%m-%d"),
                "atm_id": atm["atm_id"],
                "atm_name": atm["name"],
                "traffic_type": atm["traffic_type"],
                "total_demand": running_day_cash,
                "transaction_count": daily_txn_count,
                "day_of_week": day_of_week,
                "is_weekend": is_weekend,
                "is_holiday": is_holiday
            })

    df_txns = pd.DataFrame(transactions)
    df_daily = pd.DataFrame(daily_summaries)

    txns_path = os.path.join(output_dir, "raw_synthetic_atm_transactions.csv")
    daily_path = os.path.join(output_dir, "daily_atm_summary.csv")

    df_txns.to_csv(txns_path, index=False)
    df_daily.to_csv(daily_path, index=False)

    print(f"Generated {len(df_txns):,} raw transactions -> {txns_path}")
    print(f"Generated {len(df_daily):,} daily ATM records -> {daily_path}")

    return df_txns, df_daily


if __name__ == "__main__":
    generate_synthetic_data(num_days=90)
