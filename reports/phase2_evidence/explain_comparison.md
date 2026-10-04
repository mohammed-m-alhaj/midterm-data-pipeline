# 📊 MongoDB Explain (executionStats) Before vs After Indexes

| Query Name | Stage Before | Stage After | Docs Examined (Before) | Docs Examined (After) | Execution Time (Before) | Execution Time (After) |
|---|:---:|:---:|:---:|:---:|:---:|:---:|
| `orders_by_city_status` | `SORT` | **`LIMIT`** | 557 | **23** | 0 ms | **5 ms** |
| `orders_by_customer` | `SORT` | **`LIMIT`** | 557 | **1** | 0 ms | **5 ms** |
| `high_value_orders_by_date` | `SORT` | **`LIMIT`** | 557 | **50** | 1 ms | **6 ms** |
