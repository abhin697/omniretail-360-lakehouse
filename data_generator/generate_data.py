import argparse
import io
import os
import random
from datetime import datetime, timedelta, timezone

import boto3
import pandas as pd
from dotenv import load_dotenv
from faker import Faker

# Load environment variables from .env
load_dotenv()

fake = Faker("en_IN")
Faker.seed(42)

# Master Catalog (Fixed IDs so Fact & Dimension joins work in Gold Layer)
PRODUCTS_CATALOG = [
    ("PROD-101", "Wireless Noise-Cancelling Headphones", "Electronics", "Audio", 2800.0, 4999.0),
    ("PROD-102", "Mechanical Gaming Keyboard", "Electronics", "Peripherals", 1900.0, 3499.0),
    ("PROD-103", "Ergonomic Office Chair", "Furniture", "Seating", 5500.0, 9999.0),
    ("PROD-104", "27-Inch 4K IPS Monitor", "Electronics", "Displays", 14500.0, 22999.0),
    ("PROD-105", "Stainless Steel Insulated Bottle", "Home & Kitchen", "Drinkware", 320.0, 799.0),
    ("PROD-106", "Running Shoes Pro", "Fashion", "Footwear", 1400.0, 2999.0),
    ("PROD-107", "Cotton Casual Shirt", "Fashion", "Apparel", 450.0, 1199.0),
    ("PROD-108", "Smart Fitness Watch", "Electronics", "Wearables", 1800.0, 3999.0),
    ("PROD-109", "USB-C Multiport Hub", "Electronics", "Accessories", 850.0, 1799.0),
    ("PROD-110", "Ceramic Coffee Mug Set", "Home & Kitchen", "Dining", 280.0, 699.0),
    ("PROD-111", "Laptop Backpack Water-Resistant", "Accessories", "Bags", 750.0, 1699.0),
    ("PROD-112", "Bluetooth Portable Speaker", "Electronics", "Audio", 1100.0, 2499.0),
    ("PROD-113", "Air Fryer 4.5L Digital", "Home & Kitchen", "Appliances", 3600.0, 6499.0),
    ("PROD-114", "Yoga Mat Anti-Slip 6mm", "Sports", "Fitness", 300.0, 749.0),
    ("PROD-115", "LED Desk Lamp with Dimmer", "Home & Kitchen", "Lighting", 480.0, 1099.0),
]

CITIES = [
    ("Mumbai", "Maharashtra"),
    ("Bengaluru", "Karnataka"),
    ("Delhi", "Delhi"),
    ("Hyderabad", "Telangana"),
    ("Pune", "Maharashtra"),
    ("Chennai", "Tamil Nadu"),
    ("Gurugram", "Haryana"),
    ("Noida", "Uttar Pradesh"),
]

TIERS = ["Bronze", "Silver", "Gold", "Platinum"]
STATUSES = ["Delivered", "Shipped", "Processing", "Cancelled", "Returned"]
PAYMENTS = ["UPI", "Credit Card", "Debit Card", "Net Banking", "COD"]


def generate_products(run_date: str) -> pd.DataFrame:
    """Generates product catalog snapshot with occasional price adjustments."""
    rows = []
    for pid, name, cat, subcat, cost, price in PRODUCTS_CATALOG:
        # Simulate slight dynamic pricing variation (+/- 5%)
        price_factor = random.choice([1.0, 1.0, 1.0, 0.95, 1.05])
        rows.append({
            "product_id": pid,
            "product_name": name,
            "category": cat,
            "sub_category": subcat,
            "unit_cost": cost,
            "selling_price": round(price * price_factor, 2),
            "updated_at": f"{run_date}T05:30:00Z",
        })
    return pd.DataFrame(rows)


def generate_customers(run_date: str, batch_size: int = 30) -> pd.DataFrame:
    """Generates incremental customer updates (new + changed attributes for SCD Type 2)."""
    rows = []
    # Pool of 120 customers (CUST-1001 to CUST-1120)
    selected_ids = random.sample(range(1001, 1121), batch_size)
    for cid_num in selected_ids:
        city, state = random.choice(CITIES)
        rows.append({
            "customer_id": f"CUST-{cid_num}",
            "full_name": fake.name(),
            "email": f"user{cid_num}@example.com",
            "city": city,
            "state": state,
            "loyalty_tier": random.choice(TIERS),
            "updated_at": f"{run_date}T05:45:00Z",
        })
    return pd.DataFrame(rows)


def generate_orders(run_date: str, num_orders: int = 150) -> pd.DataFrame:
    """Generates daily orders + late status updates + intentional dirty anomalies."""
    date_obj = datetime.strptime(run_date, "%Y-%m-%d")
    prev_date = (date_obj - timedelta(days=1)).strftime("%Y-%m-%d")
    date_compact = run_date.replace("-", "")
    prev_compact = prev_date.replace("-", "")

    rows = []

    # 1. Clean daily orders
    for i in range(1, num_orders + 1):
        prod = random.choice(PRODUCTS_CATALOG)
        city, _ = random.choice(CITIES)
        qty = random.randint(1, 5)
        discount = random.choice([0, 5, 10, 15, 20])
        status = random.choices(STATUSES, weights=[55, 20, 10, 8, 7])[0]

        # Inject messy formatting in 15% of rows (Silver layer will standardize these)
        order_date_val = run_date
        if random.random() < 0.10:
            order_date_val = date_obj.strftime("%d/%m/%Y")
        if random.random() < 0.10:
            status = f"  {status.lower()}  "

        rows.append({
            "order_id": f"ORD-{date_compact}-{i:04d}",
            "order_date": order_date_val,
            "customer_id": f"CUST-{random.randint(1001, 1120)}",
            "product_id": prod[0],
            "quantity": qty,
            "unit_price": prod[5],
            "discount_pct": discount,
            "order_status": status,
            "payment_method": random.choice(PAYMENTS),
            "shipping_city": city,
            "updated_at": f"{run_date}T{random.randint(6, 22):02d}:{random.randint(0, 59):02d}:00Z",
        })

    # 2. Late-arriving updates (10 orders from yesterday changing status to Delivered/Returned)
    for j in range(1, 11):
        prod = random.choice(PRODUCTS_CATALOG)
        city, _ = random.choice(CITIES)
        rows.append({
            "order_id": f"ORD-{prev_compact}-{j:04d}",
            "order_date": prev_date,
            "customer_id": f"CUST-{random.randint(1001, 1120)}",
            "product_id": prod[0],
            "quantity": random.randint(1, 3),
            "unit_price": prod[5],
            "discount_pct": 10,
            "order_status": random.choice(["Delivered", "Returned", "Cancelled"]),
            "payment_method": random.choice(PAYMENTS),
            "shipping_city": city,
            "updated_at": f"{run_date}T04:15:00Z",
        })

    # 3. Exact duplicate rows (simulating POS/network retry)
    rows.extend(random.sample(rows[:20], 5))

    # 4. Corrupt records (Missing customer_id or negative quantity -> Quarantine in Databricks)
    corrupt_rows = [
        {
            "order_id": f"ORD-{date_compact}-9991",
            "order_date": run_date,
            "customer_id": None,
            "product_id": "PROD-101",
            "quantity": 2,
            "unit_price": 4999.0,
            "discount_pct": 0,
            "order_status": "Delivered",
            "payment_method": "UPI",
            "shipping_city": "Mumbai",
            "updated_at": f"{run_date}T10:00:00Z",
        },
        {
            "order_id": f"ORD-{date_compact}-9992",
            "order_date": run_date,
            "customer_id": "CUST-1005",
            "product_id": "PROD-104",
            "quantity": -3,
            "unit_price": 22999.0,
            "discount_pct": 5,
            "order_status": "Processing",
            "payment_method": "Credit Card",
            "shipping_city": "Delhi",
            "updated_at": f"{run_date}T11:20:00Z",
        },
    ]
    rows.extend(corrupt_rows)

    df = pd.DataFrame(rows)
    # Shuffle rows realistically
    return df.sample(frac=1).reset_index(drop=True)


def upload_df_to_s3(df: pd.DataFrame, bucket: str, s3_key: str, s3_client) -> None:
    """Converts DataFrame to CSV in memory and uploads directly to AWS S3."""
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    s3_client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=csv_buffer.getvalue(),
        ContentType="text/csv",
    )
    print(f"[SUCCESS] Uploaded {len(df)} rows -> s3://{bucket}/{s3_key}")


def main():
    parser = argparse.ArgumentParser(description="OmniRetail Daily Dirty Data Generator")
    parser.add_argument(
        "--date",
        type=str,
        default=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        help="Target load date in YYYY-MM-DD format (defaults to today UTC)",
    )
    args = parser.parse_args()
    run_date = args.date

    bucket_name = os.getenv("S3_BUCKET_NAME")
    region = os.getenv("AWS_REGION", "ap-south-1")

    if not bucket_name:
        raise ValueError("S3_BUCKET_NAME is missing in environment variables or .env file!")

    s3_client = boto3.client(
        "s3",
        region_name=region,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )

    print(f"--- Generating OmniRetail data for {run_date} ---")
    products_df = generate_products(run_date)
    customers_df = generate_customers(run_date)
    orders_df = generate_orders(run_date)

    upload_df_to_s3(products_df, bucket_name, f"products/products_{run_date}.csv", s3_client)
    upload_df_to_s3(customers_df, bucket_name, f"customers/customers_{run_date}.csv", s3_client)
    upload_df_to_s3(orders_df, bucket_name, f"orders/orders_{run_date}.csv", s3_client)
    print("--- All files uploaded successfully! ---")


if __name__ == "__main__":
    main()