# 📊 MongoDB Explain (executionStats) Before vs After Indexes

| Query Name | Stage Before | Stage After | Docs Examined (Before) | Docs Examined (After) | Execution Time (Before) | Execution Time (After) |
|---|:---:|:---:|:---:|:---:|:---:|:---:|
| `orders_by_city_status` | `SORT` | **`LIMIT`** | 540 | **20** | 0 ms | **6 ms** |
| `orders_by_customer` | `SORT` | **`LIMIT`** | 540 | **1** | 0 ms | **8 ms** |
| `high_value_orders_by_date` | `SORT` | **`LIMIT`** | 540 | **50** | 1 ms | **13 ms** |
