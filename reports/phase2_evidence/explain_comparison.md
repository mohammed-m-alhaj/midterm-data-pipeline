# 📊 MongoDB Explain (executionStats) Before vs After Indexes

| Query Name | Stage Before | Stage After | Docs Examined (Before) | Docs Examined (After) | Execution Time (Before) | Execution Time (After) | Index Used |
|---|:---:|:---:|:---:|:---:|:---:|:---:|---|
| `orders_by_city_status` | `COLLSCAN → SORT` | **`IXSCAN → FETCH → LIMIT`** | 1,380 | **50** | 1 ms | **6 ms** | `idx_city_status_total_amount` |
| `orders_by_customer` | `COLLSCAN → SORT` | **`IXSCAN → FETCH → LIMIT`** | 1,380 | **1** | 1 ms | **6 ms** | `idx_customer_id_order_date` |
| `high_value_orders_by_date` | `COLLSCAN → SORT` | **`IXSCAN → FETCH → LIMIT`** | 1,380 | **50** | 3 ms | **7 ms** | `idx_order_date_total_amount` |
