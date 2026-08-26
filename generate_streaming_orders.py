"""
generate_streaming_orders.py

Simulates a real-time order feed for the Databricks streaming project.

WHAT IT DOES
------------
Every 2-5 seconds it writes ONE new JSON file containing 1-5 order records
into an output folder. This mimics an upstream order-management system
that continuously drops files into cloud storage (S3 / ADLS / GCS), which
Databricks Auto Loader then picks up.

HOW TO RUN
----------
Inside a Databricks notebook:
    Just paste this code into a cell (or import the file) and run it.
    Set OUTPUT_DIR to a DBFS / Unity Catalog Volume path, e.g.:
        OUTPUT_DIR = "/Volumes/main/retail/landing/streaming_orders"
    or on Community Edition:
        OUTPUT_DIR = "/dbfs/FileStore/streaming_orders"

On your local machine:
    pip install -r requirements.txt
    python data_generator/generate_streaming_orders.py
    (writes to ./streaming_orders locally; upload to DBFS with the
     Databricks CLI: databricks fs cp -r ./streaming_orders dbfs:/FileStore/streaming_orders)

Stop anytime with Ctrl+C. It runs forever by design (real-time simulation).
"""

import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

try:
    from faker import Faker
    fake = Faker()
except ImportError:
    fake = None

OUTPUT_DIR = os.environ.get("ORDERS_OUTPUT_DIR", "./streaming_orders")
MIN_SLEEP_SECONDS = 2
MAX_SLEEP_SECONDS = 5
MIN_ORDERS_PER_FILE = 1
MAX_ORDERS_PER_FILE = 5

CATEGORIES = ["Electronics", "Clothing", "Home & Kitchen", "Sports",
              "Books", "Beauty", "Toys", "Grocery"]
PRODUCTS = {
    "Electronics": ["Wireless Earbuds", "Bluetooth Speaker", "Smartwatch", "Laptop Stand"],
    "Clothing": ["Cotton T-Shirt", "Denim Jacket", "Running Shoes", "Wool Sweater"],
    "Home & Kitchen": ["Air Fryer", "Coffee Maker", "Blender", "Cutlery Set"],
    "Sports": ["Yoga Mat", "Dumbbell Set", "Cricket Bat", "Football"],
    "Books": ["Data Engineering Guide", "Mystery Novel", "Cook Book", "Self-Help Book"],
    "Beauty": ["Face Serum", "Lipstick", "Shampoo", "Perfume"],
    "Toys": ["Building Blocks", "RC Car", "Puzzle Set", "Action Figure"],
    "Grocery": ["Organic Rice 5kg", "Olive Oil 1L", "Green Tea Pack", "Almonds 500g"],
}
REGIONS = ["North", "South", "East", "West", "Central"]
PAYMENT_METHODS = ["UPI", "Credit Card", "Debit Card", "Net Banking", "Cash on Delivery"]


def random_customer_name():
    if fake:
        return fake.name()
    first = random.choice(["Aarav", "Vivaan", "Priya", "Ananya", "Rohan",
                            "Ishaan", "Diya", "Kabir", "Meera", "Arjun"])
    last = random.choice(["Sharma", "Iyer", "Patel", "Reddy", "Nair",
                           "Gupta", "Menon", "Rao", "Singh", "Verma"])
    return f"{first} {last}"


def build_order():
    category = random.choice(CATEGORIES)
    product = random.choice(PRODUCTS[category])
    quantity = random.randint(1, 4)
    unit_price = round(random.uniform(199, 15999), 2)
    # inject a small % of "dirty" records so the Silver layer has real work to do
    is_dirty = random.random() < 0.05

    order = {
        "order_id": str(uuid.uuid4()),
        "customer_name": random_customer_name() if not is_dirty else None,
        "product_name": product,
        "category": category,
        "quantity": quantity if not is_dirty else -1,
        "unit_price": unit_price,
        "total_amount": round(quantity * unit_price, 2),
        "region": random.choice(REGIONS),
        "payment_method": random.choice(PAYMENT_METHODS),
        "order_status": random.choices(
            ["PLACED", "CONFIRMED", "SHIPPED", "DELIVERED", "CANCELLED"],
            weights=[10, 20, 25, 40, 5],
        )[0],
        "order_timestamp": datetime.now(timezone.utc).isoformat(),
    }
    return order


def write_batch_file():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    n_orders = random.randint(MIN_ORDERS_PER_FILE, MAX_ORDERS_PER_FILE)
    orders = [build_order() for _ in range(n_orders)]

    filename = f"orders_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%f')}.json"
    filepath = os.path.join(OUTPUT_DIR, filename)

    # one JSON object per line (standard for Auto Loader JSON ingestion)
    with open(filepath, "w") as f:
        for order in orders:
            f.write(json.dumps(order) + "\n")

    return filepath, n_orders


def main():
    print(f"Streaming order generator started. Writing to: {OUTPUT_DIR}")
    print("Press Ctrl+C to stop.\n")
    total_files = 0
    total_orders = 0
    try:
        while True:
            filepath, n_orders = write_batch_file()
            total_files += 1
            total_orders += n_orders
            print(f"[{datetime.now().strftime('%H:%M:%S')}] wrote {filepath} "
                  f"({n_orders} orders) | totals: {total_files} files, {total_orders} orders")
            time.sleep(random.uniform(MIN_SLEEP_SECONDS, MAX_SLEEP_SECONDS))
    except KeyboardInterrupt:
        print(f"\nStopped. Generated {total_files} files / {total_orders} orders total.")


if __name__ == "__main__":
    main()
