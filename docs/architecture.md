# 🏛️ Architecture & System Design Document

This document provides a comprehensive technical overview of the **Enterprise Hybrid Data Pipeline** designed for the Big Data Practical Midterm Project (Razi University).

---

## 1. High-Level Pipeline Architecture

```text
Dirty CSV Input
       |
       v
+-------------------------------------------------------------+
| File Router: Inspect Size against 200 MB Config Threshold    |
+------------------------------+------------------------------+
                               |
            +------------------+------------------+
            | (Size <= 200 MB)                    | (Size > 200 MB)
            v                                     v
+-----------------------+             +-----------------------+
|  Streaming Python     |             |  Distributed PySpark  |
|  Batch Loader         |             |  Parallel Loader      |
|  (csv.DictReader)     |             |  (Partitions & Tasks) |
+-----------+-----------+             +-----------+-----------+
            |                                     |
            +------------------+------------------+
                               |
                               v
               +-------------------------------+
               |      MongoDB: orders_raw      |
               | (Zero Data Loss + Lineage)    |
               +---------------+---------------+
                               |
                               v
               +-------------------------------+
               |   PySpark ELT Quality Engine  |
               |   (9 Transformation Rules)    |
               +---------------+---------------+
                               |
            +------------------+------------------+
            | (Valid / Corrected)                 | (Irreparable Errors)
            v                                     v
+-------------------------------+     +-------------------------------+
|   MongoDB: orders_validated   |     |   MongoDB: orders_quarantine  |
|   (Idempotent Upsert & Hash)  |     |   (Error Codes & Diagnostics) |
+---------------+---------------+     +---------------+---------------+
                |                                     |
                +------------------+------------------+
                                   |
                                   v
                   +-------------------------------+
                   |      reports/results.json     |
                   |      (Metrics & Benchmarks)   |
                   +-------------------------------+
```

---

## 2. Layer Contracts & Guarantees

### A. Raw Ingestion Layer Contract (`orders_raw`)
- **Append-Oriented Storage:** Unmodified retention of all records. No database constraints or validators reject bad records at this stage.
- **Lineage Metadata Attachment:**
  - `run_id`: Unique UUID per execution.
  - `source_file`: Absolute source file path.
  - `source_row_number`: Exact 1-based CSV line number.
  - `ingested_at`: UTC timestamp.
  - `engine_used`: `python_batch` or `pyspark`.
  - `raw_record`: Raw unparsed JSON string preserving exact input payload.

### B. Validated Final State Contract (`orders_validated`)
- **Stable Business Key:** `order_id` is the primary business identifier.
- **Database Index:** Unique index `uq_validated_order_id` strictly enforced.
- **Atomic Upsert:** Written with `operationType=replace`, `upsertDocument=true`, and `idFieldList=order_id`.
- **Audit Trail:** Full array of `corrections` documenting modified fields:
  - `field`: Name of the modified column.
  - `original_value`: Input dirty value.
  - `corrected_value`: Cleaned standardized value.
  - `rule_code`: Specific rule identifier (e.g., `PHONE_NORMALIZE`, `DATE_STANDARDIZE`).
- **Cryptographic Hash:** `record_hash` computed via `SHA-256` over all standardized columns.

### C. Quarantine Layer Contract (`orders_quarantine`)
- **Zero Loss Guarantee:** Corrupted or unfixable records are never dropped silently.
- **Explicit Diagnostics:**
  - `error_codes`: Array of triggered error tags (`MISSING_ORDER_ID`, `INVALID_IMPOSSIBLE_DATE`, `CORRUPTED_ITEMS_JSON`, `UNKNOWN_PRICE`, `DUPLICATE_ORDER_ID`, etc.).
  - `error_details`: Comma-separated summary string.
  - `raw_record`: Original unparsed record preserved for manual inspection.

### D. Run Consistency Invariant (Mathematical Guarantee)
For every pipeline run, the following invariant is evaluated via automated assertion:
$$\text{raw\_count} = \text{valid\_count} + \text{corrected\_count} + \text{quarantine\_count}$$

---

## 3. The 9 Deterministic Quality Cleaning Rules

1. **Arabic Digits Conversion:** `٠١٢٣٤٥٦٧٨٩` translated to standard ASCII `0123456789`.
2. **Currency Standardization:** Textual currency tags (`ريال`, `ر.ي`, `YER`) stripped and standardized to code `YER`.
3. **Thousands Separator Removal:** Commas (`,`) and Arabic decimal points (`٫`) converted to clean decimal points.
4. **Textual Word Prices:** Converts known word prices ("ألفان" $\rightarrow 2000$, "خمسة آلاف" $\rightarrow 5000$, "عشرة آلاف" $\rightarrow 10000$).
5. **Phone Number Normalization:** Standardizes Yemeni phone prefixes (`00967`, `0967`, `967`, `07`, `7`) into international standard `+9677XXXXXXXX`.
6. **Email Symbol Repair:** Repairs repeated symbols (`@@+` $\rightarrow$ `@`, `\.{2,}` $\rightarrow$ `.`) and enforces lowercase.
7. **Date Format Standardization:** Parses diverse date formats (`yyyy-MM-dd`, `dd/MM/yyyy`, `dd-MM-yyyy`, ISO) into standard ISO timestamp strings.
8. **Status Synonym Mapping:** Trims whitespace and normalizes synonyms ("مدفوع" / "دفع" $\rightarrow$ "تم الدفع", "غير مدفوع" $\rightarrow$ "بانتظار الدفع").
9. **Total Recalculation:** Verifies $Total = \sum Items + Delivery$ and corrects erroneous totals when items and delivery prices are valid.

---

## 4. Idempotency & In-Place Mutation Mechanics

```text
[Input Row] -> [Clean Row] -> [Generate SHA-256 Hash]
                                        |
                         +--------------+--------------+
                         |                             |
                  (order_id New)              (order_id Found in DB)
                         |                             |
                         v                             v
                  [Insert New Doc]             [Compare SHA-256 Hash]
                  inserted_count + 1                   |
                                        +--------------+--------------+
                                        |                             |
                                 (Hash Changed)               (Hash Identical)
                                        |                             |
                                        v                             v
                                 [Replace Doc]                 [Skip / No-Op]
                                 updated_count + 1             unchanged_count + 1
```

- When the exact same file is re-run: `inserted_count = 0`, `updated_count = 0`, `unchanged_count = N`.
- When an existing order has updated fields: `inserted_count = 0`, `updated_count = 1`, `unchanged_count = N - 1`. Total collection document count remains constant.
