# دليل لقطات الشاشة المطلوبة للتسليم

## 📸 لقطات مطلوبة حسب الوثيقة (القسم 10)

> التقط هذه اللقطات أثناء تشغيل المشروع وضعها في `reports/screenshots/`

---

### 1. Spark Master UI — واجهة الماستر
- **المسار**: `http://localhost:8080` (أو IP الماستر)
- **ما يجب إظهاره**:
  - حالة Workers المتصلين
  - الذاكرة والأنوية المتاحة
  - التطبيقات قيد التشغيل / المكتملة
- **اسم الملف**: `spark_master_ui.png`

### 2. Spark Application UI — Jobs & Stages
- **المسار**: `http://localhost:4040` (أثناء تشغيل التطبيق)
- **ما يجب إظهاره**:
  - **Jobs**: قائمة الوظائف المكتملة
  - **Stages**: المراحل وعدد Tasks في كل مرحلة
  - **Tasks**: توزيع المهام على Executors
  - **Partitions**: عدد الأقسام (يجب أن يكون 8 = `SPARK_PARTITIONS`)
- **أسماء الملفات**:
  - `spark_jobs.png`
  - `spark_stages.png`
  - `spark_tasks.png`

### 3. MongoDB Collections — مجموعات البيانات
- **الأداة**: MongoDB Compass أو `mongosh`
- **ما يجب إظهاره**:
  - **orders_raw**: سجل خام مع `run_id`, `source_file`, `raw_record`
  - **orders_validated**: سجل مع `quality_status: "corrected"` و `corrections` array
  - **orders_quarantine**: سجل مع `error_codes` و `error_details`
  - **عدد الوثائق** في كل مجموعة
- **أسماء الملفات**:
  - `mongo_orders_raw.png`
  - `mongo_orders_validated.png`
  - `mongo_orders_quarantine.png`
  - `mongo_collection_counts.png`

### 4. Idempotency & Upsert — إثبات عملي
- **تشغيل 1**: أول تشغيل → لقطة `inserted_count > 0`
- **تشغيل 2**: إعادة نفس البيانات → لقطة `inserted_count = 0, unchanged_count > 0`
- **تشغيل 3 (Update)**: تعديل سجل → لقطة `updated_count > 0` بدون Duplicate
- **أسماء الملفات**:
  - `idempotency_first_run.png`
  - `idempotency_second_run.png`
  - `upsert_update_demo.png`

### 5. File Router — اختيار المحرك
- **ما يجب إظهاره**:
  - تشغيل العينة الصغيرة → يختار `python_batch`
  - تشغيل الملف الكبير → يختار `pyspark`
- **أسماء الملفات**:
  - `router_python_batch.png`
  - `router_pyspark.png`

---

## 🔧 أوامر مساعدة لتوليد الدليل

### عرض عدد الوثائق في MongoDB:
```javascript
// في mongosh
use midterm_pipeline
db.orders_raw.countDocuments()
db.orders_validated.countDocuments()
db.orders_quarantine.countDocuments()
```

### عرض مثال سجل مصحح مع Audit Trail:
```javascript
db.orders_validated.findOne({quality_status: "corrected", corrections: {$ne: []}})
```

### عرض مثال سجل معزول مع أسباب:
```javascript
db.orders_quarantine.findOne({error_codes: {$exists: true}})
```

### إثبات Unique Index:
```javascript
db.orders_validated.getIndexes()
// يجب أن يظهر: { order_id: 1 } unique: true
```

### إثبات Schema Validation:
```javascript
db.getCollectionInfos({name: "orders_validated"})
// يجب أن يظهر: validator.$jsonSchema
```

---

## 📝 تجربة Update الموثقة (لإثبات updated_count)

### الخطوات:
1. شغّل الـ pipeline أول مرة → سجّل `inserted_count`
2. شغّل مرة ثانية نفس البيانات → يجب أن يكون `inserted_count=0, unchanged_count>0`
3. عدّل سجل واحد في MongoDB مباشرة:
```javascript
// غيّر record_hash لسجل واحد لمحاكاة تحديث
db.orders_validated.updateOne(
  {order_id: "ORD-00001"},
  {$set: {record_hash: "MODIFIED_FOR_TEST"}}
)
```
4. شغّل الـ pipeline مرة ثالثة → يجب أن يظهر `updated_count: 1`
5. التقط لقطة شاشة تظهر الـ 3 نتائج

### النتيجة المتوقعة:
| التشغيل | inserted | updated | unchanged |
|---|---|---|---|
| الأول | >0 | 0 | 0 |
| الثاني (نفس البيانات) | 0 | 0 | >0 |
| الثالث (بعد تعديل hash) | 0 | 1 | >0 |
