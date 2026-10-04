from __future__ import annotations

import csv
import json
import random
import time
from datetime import datetime, timedelta
from pathlib import Path
from uuid import uuid4

from bootstrap import PROJECT_ROOT, ensure_project_root

ensure_project_root()

from config.settings import DATA_DIR, RAW_COLUMNS

CITIES = ["صنعاء", "عدن", "تعز", "الحديدة", "إب", "المكلا", "ذمار"]
DISTRICTS = ["السبعين", "المنصورة", "صالة", "الحوك", "المشنة", "المكلا", "عنس"]
STATUSES = ["مؤكد", "مدفوع", "دفع", "بانتظار الدفع", "غير مدفوع", "قيد الشحن"]
PAYMENT_METHODS = ["كاش", "بطاقة", "محفظة إلكترونية", "تحويل بنكي"]
DELIVERY_TYPES = ["سريع", "عادي"]
CURRENCIES = ["YER", "ريال", "ريال يمني", "ر.ي"]

ARABIC_DIGIT_MAP = str.maketrans("0123456789", "٠١٢٣٤٥٦٧٨٩")


def generate_dynamic_dataset(num_rows: int = 350) -> Path:
    """Generates a randomized, dynamic dataset with various real-world edge cases."""
    timestamp = int(time.time())
    unique_suffix = uuid4().hex[:6]
    file_path = DATA_DIR / f"dynamic_phase2_{timestamp}_{unique_suffix}.csv"

    base_date = datetime(2026, 1, 1)

    rows = []
    for i in range(1, num_rows + 1):
        order_num = f"DYN-{unique_suffix.upper()}-{i:05d}"
        cust_id = f"CUST-DYN-{random.randint(100, 180):04d}"
        cust_name = f"عميل تجريبي {random.randint(1, 50)}"

        # Randomized date
        delta_days = random.randint(0, 180)
        row_date = (base_date + timedelta(days=delta_days)).strftime("%Y-%m-%d")

        # Introduce realistic dirty variations
        phone_seed = random.randint(770000000, 779999999)
        if i % 5 == 0:
            # Arabic digits phone
            phone = str(phone_seed).translate(ARABIC_DIGIT_MAP)
        elif i % 7 == 0:
            phone = f"00967{phone_seed}"
        else:
            phone = f"+967{phone_seed}"

        email_clean = f"user_{unique_suffix}_{i}@sample-store.ye"
        if i % 6 == 0:
            email = f"user_{unique_suffix}_{i}@@sample-store..ye"
        else:
            email = email_clean

        city = random.choice(CITIES)
        district = random.choice(DISTRICTS)
        delivery_type = random.choice(DELIVERY_TYPES)
        delivery_cost = float(random.choice([1000, 1500, 2000, 2500, 3000]))

        unit_price = float(random.choice([2000, 3500, 5000, 7500, 12000]))
        qty = random.randint(1, 4)
        item_total = unit_price * qty
        items_payload = json.dumps([
            {
                "sku": f"SKU-{random.randint(10, 99)}",
                "name": f"منتج تجريبي {random.randint(1, 20)}",
                "qty": qty,
                "unit_price": unit_price,
                "total": item_total,
            }
        ], ensure_ascii=False)

        total_amount = item_total + delivery_cost
        payment_amount = total_amount
        payment_method = random.choice(PAYMENT_METHODS)
        status = random.choice(STATUSES)
        payment_status = "تم الدفع" if status in ("تم الدفع", "مدفوع", "دفع") else "بانتظار الدفع"

        currency = random.choice(CURRENCIES)

        # Money formatting variation
        if i % 8 == 0:
            cost_str = f"{int(delivery_cost):,} ريال يمني"
        elif i % 9 == 0:
            cost_str = str(int(delivery_cost)).translate(ARABIC_DIGIT_MAP)
        else:
            cost_str = str(delivery_cost)

        rows.append({
            "order_id": order_num,
            "order_date": row_date,
            "status": status,
            "customer_id": cust_id,
            "customer_name": cust_name,
            "customer_phone": phone,
            "customer_email": email,
            "city": city,
            "district": district,
            "delivery_type": delivery_type,
            "delivery_cost": cost_str,
            "payment_method": payment_method,
            "payment_status": payment_status,
            "payment_amount": str(payment_amount),
            "currency": currency,
            "total_amount": str(total_amount),
            "items_json": items_payload,
        })

    with open(file_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=RAW_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)

    return file_path


if __name__ == "__main__":
    generated_file = generate_dynamic_dataset(300)
    print(f"Generated dynamic dataset: {generated_file} ({generated_file.stat().st_size} bytes)")
