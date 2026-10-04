# 📊 MongoDB Explain (executionStats) Before vs After Indexes

| Query Name | Stage Before | Stage After | Docs Examined (Before) | Docs Examined (After) | Execution Time (Before) | Execution Time (After) |
|---|:---:|:---:|:---:|:---:|:---:|:---:|
| `orders_by_city_status` | `SORT` | **`LIMIT`** | 986 | **45** | 1 ms | **6 ms** |
| `orders_by_customer` | `SORT` | **`LIMIT`** | 986 | **1** | 1 ms | **7 ms** |
| `high_value_orders_by_date` | `SORT` | **`LIMIT`** | 986 | **50** | 2 ms | **7 ms** |
