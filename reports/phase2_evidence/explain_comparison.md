# 📊 MongoDB Explain (executionStats) Before vs After Indexes

| Query Name | Stage Before | Stage After | Docs Examined (Before) | Docs Examined (After) | Execution Time (Before) | Execution Time (After) |
|---|:---:|:---:|:---:|:---:|:---:|:---:|
| `orders_by_city_status` | `SORT` | **`LIMIT`** | 101 | **0** | 0 ms | **5 ms** |
| `orders_by_customer` | `SORT` | **`LIMIT`** | 101 | **1** | 0 ms | **4 ms** |
| `high_value_orders_by_date` | `SORT` | **`LIMIT`** | 101 | **50** | 0 ms | **15 ms** |
