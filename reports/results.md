# 🚀 تقرير نتائج اختبار وتوثيق مشروع البيانات الضخمة (Midterm Data Pipeline Report)

**جامعة الرازي | مقرر البيانات الضخمة (العملي)**  
**المشروع:** خط بيانات هجين لمقاييس المؤسسات (Streaming Python Batch + Apache Spark Standalone Cluster + MongoDB + Automated Data Quality ELT + Idempotency & Upsert)  
**المسار المنفذ:** **Path A (Spark Standalone Cluster على جهاز واحد وفق توجيهات المشرف الأكاديمي) + المسار الأساسي (Python Batch Streaming & Small Sample)**  
**حجم المعالجة الفعلي:** **1,000,000 سجل عبر Spark Standalone (418.55 MB)** و **5,000 سجل عبر Python Batch Streaming (2.09 MB)**  

---

## 📌 1. المعمارية الهجينة واستراتيجية التوجيه (Hybrid File Router Architecture)

تم بناء وتنفيذ **خط بيانات هجين (Enterprise Hybrid Data Pipeline)** يوجه البيانات تلقائياً وفق حجم الملف:
- **الملفات $\le 200\text{ MB}$:** تُعالج عبر `python_batch` باستخدام القراءة المتدفقة `csv.DictReader` والإدراج على دفعات في MongoDB دون تحميل الملف بالذاكرة ($O(1) \text{ Memory}$).
- **الملفات $> 200\text{ MB}$:** تُعالج عبر `pyspark` على عنقود **Spark Standalone Cluster** المتصل عبر الرابط `spark://127.0.0.1:7077` مع إعادة توزيع البيانات على 8 تقسيمات (`SPARK_PARTITIONS = 8`) لتحقيق التوازي الكامل على أنوية الـ Worker.

```
                           +--------------------------------+
                           |     Dirty Input CSV File       |
                           +---------------+----------------+
                                           |
                                           v
                           +---------------+----------------+
                           |  File Router (Threshold 200MB) |
                           +-------+----------------+-------+
                                   |                |
                       <= 200MB    |                | > 200MB
                                   v                v
                        +----------+---+   +--------+---------+
                        | Python Batch |   | PySpark          |
                        | Streaming    |   | Standalone Master|
                        | (O(1) Memory)|   | (127.0.0.1:7077) |
                        +----------+---+   +--------+---------+
                                   |                |
                                   +-------+--------+
                                           |
                                           v
                           +---------------+----------------+
                           |    MongoDB: orders_raw         | (Zero-Loss Raw Ingestion)
                           +---------------+----------------+
                                           |
                                           v
                           +---------------+----------------+
                           | PySpark ELT Quality Pipeline   | (9 Deterministic Rules)
                           +---------------+----------------+
                                           |
                                           v
                           +---------------+----------------+
                           | Quality Status Classification  |
                           +-------+----------------+-------+
                                   |                |
                       Valid /     |                | Invalid /
                      Corrected    v                v Unfixable
                        +----------+---+   +--------+---------+
                        |   orders_    |   |    orders_      |
                        |  validated   |   |   quarantine    |
                        | (Idempotent  |   | (Error Codes &  |
                        |   Upsert)    |   |  Raw Lineage)   |
                        +--------------+   +-----------------+
```

---

## ⚖️ 2. جدول المقارنة الشاملة بين المحركين (Comparative Benchmark: Python Batch vs PySpark)

| المقارنة / الخاصية | محرك الدفعات في بايثون (Python Batch Streaming) | محرك سبارك الموزع (PySpark Standalone Cluster) |
|---|---|---|
| **حجم الملف النموذجي** | $2.09\text{ MB}$ (`orders_small_sample.csv`) | $418.55\text{ MB}$ (`orders_1m_sample.csv`) |
| **عدد السجلات المعالجة** | $5,000\text{ records}$ | **$1,000,000\text{ records}$** |
| **آلية الاستهلاك بالذاكرة** | قراءة متدفقة سطراً بسطر ($O(1)\text{ RAM}$) عبر `csv.DictReader` | قراءة موزعة RDD/DataFrame مع تقييم كسلان (Lazy Evaluation) |
| **حجم الدفعة / التقسيمات** | `BATCH_SIZE = 1,000` سجل لكل عملية `insert_many` | `8 Output Partitions` موزعة عبر `repartition(8)` |
| **زمن التحميل الخام (Raw Load)** | **`0.13 s`** (5 دفعات @ 1000 سجل) | **`32.19 s`** (8 تقسيمات متوازية) |
| **معدل المعالجة (Throughput)** | **`37,693.29 rows/s`** | **`31,065.67 rows/s`** |
| **عنوان المحرك (Master URL)** | `N/A` (Local Python Process) | `spark://127.0.0.1:7077` (Standalone Master) |
| **الاستخدام الأمثل** | الملفات الصغيرة والعمليات المباشرة الخفيفة | معالجة البيانات الضخمة، والتحويلات المعقدة والمتوازية |
| **دليل الإثبات الصوري** | [`12_python_batch_streaming.png`](screenshots/12_python_batch_streaming.png) | [`09_idempotency_run1.png`](screenshots/09_idempotency_run1.png) |

---

## 🧪 3. إثبات العينة الصغيرة المؤتمتة (Reproducible Small Sample)

تم استخراج العينة الصغيرة برمجياً بواسطة سكربت مستقل غير يدوي:
- **السكربت المنفذ:** `python src/create_small_sample.py --rows 5000`
- **الملف المصدر:** `data/orders_huge_mixed_quality.csv` (13.26 GB)
- **الملف الناتج:** `data/orders_small_sample.csv` (2.09 MB، 5,000 صف بيانات)
- **مخرجات السكربت:**
```text
============================================================
SMALL SAMPLE CREATION
============================================================
Source file : C:\Users\Al-Haj\Desktop\midterm-data-pipeline\data\orders_huge_mixed_quality.csv
Output file : C:\Users\Al-Haj\Desktop\midterm-data-pipeline\data\orders_small_sample.csv
Rows        : 5000
Requested   : 5000
============================================================
```

---

## ⚡ 4. إثبات التدفق المباشر لبايثون (Python Batch Streaming Execution)

تم تمرير ملف العينة الصغيرة عبر الموجه العام:
- **الأمر المنفذ:** `python src/main.py --file data/orders_small_sample.csv`
- **قرار الموجه (Router):** `python_batch` (لأن 2.09 MB $\le$ 200 MB)
- **تفاصيل الدفعات المسجلة:**
  - الدفعة 1: `rows=1,000` | `elapsed=0.02s` | `rate=56,377.1 rows/s`
  - الدفعة 2: `rows=1,000` | `elapsed=0.01s` | `rate=71,365.9 rows/s`
  - الدفعة 3: `rows=1,000` | `elapsed=0.01s` | `rate=71,891.8 rows/s`
  - الدفعة 4: `rows=1,000` | `elapsed=0.02s` | `rate=62,480.9 rows/s`
  - الدفعة 5: `rows=1,000` | `elapsed=0.01s` | `rate=68,579.6 rows/s`
- **إجمالي الوقت:** `0.13 s` | **الإجمالي المحقون في orders_raw:** `5,000 docs` | **أخطاء الدفعات:** `0`
- **معادلة الاتساق للعينة الصغيرة:**
  $$5,000 (\text{Raw}) = 0 (\text{Valid}) + 4,254 (\text{Corrected}) + 746 (\text{Quarantine}) \quad \checkmark \text{ [MATCH 100\%]}$$

---

## 🧹 5. مصفوفة قواعد تنظيف وتصنيف البيانات (9 Automated Quality Rules)

تم تطبيق واختبار 9 قواعد تنظيف حتمية غير تخمينية معتمدة ومثبتة بحزمة اختبارات PyTest (33 Passed / 100% Pass Rate):

| # | اسم القاعدة والهدف | رمز القاعدة | القيمة قبل التنظيف (Dirty CSV) | القيمة بعد التنظيف (Normalized) | حالة اختبار PyTest |
|---|---|---|---|---|:---:|
| 1 | **تحويل الأرقام العربية** | `ARABIC_DIGITS` | `٧٠٦٠٠٠٫٠` | `706000.0` | **PASSED** |
| 2 | **توحيد مسميات العملات** | `CURRENCY_STANDARDIZE` | `5000 ريال يمني` / `2500 ر.ي` | `YER` (الرقم: `5000` / `2500`) | **PASSED** |
| 3 | **حذف فواصل الآلاف والنقاط** | `MONEY_NORMALIZE` | `١٢٥,٠٠٠.00` / `125,000.00` | `125000.00` | **PASSED** |
| 4 | **تحويل الأسعار النصية المعروفة** | `WORD_PRICE_MAP` | `ألفان` / `خمسة آلاف` / `عشرة آلاف` | `2000` / `5000` / `10000` | **PASSED** |
| 5 | **توحيد صيغة الهاتف اليمني** | `PHONE_NORMALIZE` | `77 123 4567` / `٩٦٧٧٧١٢٣٤٥٦٧` | `+967771234567` | **PASSED** |
| 6 | **إصلاح رموز البريد المكررة** | `EMAIL_REPEATED_SYMBOLS` | `user@@mail..com` | `user@mail.com` | **PASSED** |
| 7 | **توحيد صيغ التواريخ** | `DATE_STANDARDIZE` | `31/01/2025` / `31-01-2025` | `2025-01-31T00:00:00` (ISO) | **PASSED** |
| 8 | **توحيد مرادفات الحالات** | `STATUS_STANDARDIZE` | `مدفوع` / `دفع` / `غير مدفوع` | `تم الدفع` / `بانتظار الدفع` | **PASSED** |
| 9 | **إعادة احتساب إجمالي الطلب** | `TOTAL_RECALCULATE` | $Items: 12000, Deliv: 1000, Tot: 99999$ | $Total = 13000.0$ ($Total = \sum Items + Deliv$) | **PASSED** |

- **دليل الإثبات الصوري لقواعد الجودة:** [`13_quality_rules_proof.png`](screenshots/13_quality_rules_proof.png)

---

## 📊 6. نتائج تشغيل Path A على مليون سجل (Path A 1M Spark Standalone)

- **الملف المعالج:** `data/orders_1m_sample.csv` (1,000,000 صف، 418.55 MB).
- **الـ Spark Master:** `spark://127.0.0.1:7077` (Spark Standalone Cluster).
- **زمن التحميل الخام (Raw Load):** `32.19 s` (Throughput: `31,065.67 rows/s`).
- **التقسيمات (Partitions):** `repartition(8)` مثبتة بـ `explain(True)` مع `RoundRobinPartitioning(8)`.
- **زمن التحويل وتصنيف الجودة (ELT):** `106.80 s` (Throughput: `9,363.24 rows/s`).
- **معادلة الاتساق للمليون سجل:**
  $$1,000,000 (\text{Raw}) = 0 (\text{Valid}) + 858,599 (\text{Corrected}) + 141,401 (\text{Quarantine}) \quad \checkmark \text{ [MATCH 100\%]}$$

---

## 🔒 7. إثبات عدم التكرار والـ Upsert والتحديث الموضعي (Idempotency & Update Proof)

| التجربة | السجلات المعالجة | inserted_count | updated_count | unchanged_count | إجمالي وثائق orders_validated | النتيجة الفنية |
|---|:---:|:---:|:---:|:---:|:---:|---|
| **Run 1 (التشغيل الأولي لمليون سجل)** | 1,000,000 | **858,599** | 0 | 0 | **858,599** | إدراج أولي كامل للسجلات السليمة والمصححة |
| **Run 2 (إعادة تشغيل نفس المليون سجل)** | 1,000,000 | **0** | 0 | **858,599** | **858,599** | **صفر سجلات مكررة (100% Idempotent)** |
| **Update Test (تعديل طلب موجود)** | 1 | **0** | **1** | 0 | **858,599** | **تحديث موضعي مباشر وتوليد هاش جديد دون أي تكرار** |

---

## 📸 8. فهرس ملفات الأدلة والصور (13 Real Screenshots Index)

| رقم الدليل | اسم الملف | الوصف ومحتوى الإثبات |
|---|---|---|
| 01 | [`01_master_worker_alive.png`](screenshots/01_master_worker_alive.png) | واجهة Spark Master UI على `http://127.0.0.1:8080` مع ظهور Worker بحالة ALIVE |
| 02 | [`02_spark_application.png`](screenshots/02_spark_application.png) | واجهة Spark Application UI على المنفذ `4040` توضح تشغيل التطبيق على العنقود |
| 03 | [`03_executors.png`](screenshots/03_executors.png) | قائمة الـ Executors وذاكرة كل منفذ والأنوية المخصصة |
| 04 | [`04_jobs_stages_tasks.png`](screenshots/04_jobs_stages_tasks.png) | مراحل المعالجة (Jobs & Stages & Tasks) وتوزيع الـ 8 تقسيمات |
| 05 | [`05_repartition_explain.png`](screenshots/05_repartition_explain.png) | الخطة الفيزيائية `explain(True)` تثبت `RoundRobinPartitioning(8)` |
| 06 | [`06_mongodb_raw.png`](screenshots/06_mongodb_raw.png) | عينة وثيقة `orders_raw` في MongoDB مع كامل بيانات التتبع `run_id`, `source_file` |
| 07 | [`07_mongodb_validated.png`](screenshots/07_mongodb_validated.png) | عينة وثيقة `orders_validated` مع مصفوفة التصحيحات `corrections` والهاش |
| 08 | [`08_mongodb_quarantine.png`](screenshots/08_mongodb_quarantine.png) | عينة وثيقة `orders_quarantine` مع تفاصيل ورموز الأخطاء `error_codes` |
| 09 | [`09_idempotency_run1.png`](screenshots/09_idempotency_run1.png) | مقاييس التشغيل الأول لمليون سجل (`inserted_count = 858,599`) |
| 10 | [`10_idempotency_run2.png`](screenshots/10_idempotency_run2.png) | مقاييس التشغيل الثاني لمليون سجل (`inserted_count = 0`, `unchanged = 858,599`) |
| 11 | [`11_update_evidence.png`](screenshots/11_update_evidence.png) | إثبات التحديث الموضعي للطلب المعدل (`updated_count = 1` بدون تكرار) |
| 12 | [`12_python_batch_streaming.png`](screenshots/12_python_batch_streaming.png) | إثبات التدفق المباشر لبايثون على دفعات (5 دفعات @ 1000 سجل) بمعدل 37,693 سجل/ثانية |
| 13 | [`13_quality_rules_proof.png`](screenshots/13_quality_rules_proof.png) | مصفوفة الـ 9 قواعد لتنظيف وتصنيف البيانات واجتياز الـ 33 اختباراً بنسبة 100% |

---

## 🏆 9. مصفوفة التحقق الشامل للمشروع كاملًا (100% Complete)

| المتطلب (Requirement) | Implemented | Executed | مسار الدليل الحقيقي (Evidence Path) |
|---|:---:|:---:|---|
| **Reproducible Small Sample** | **Yes** | **Yes** | `src/create_small_sample.py` (`data/orders_small_sample.csv`) |
| **Router -> Python Batch ($\le 200\text{MB}$)** | **Yes** | **Yes** | `src/file_router.py` / [`12_python_batch_streaming.png`](screenshots/12_python_batch_streaming.png) |
| **Python Batch Streaming ($O(1)\text{ RAM}$)** | **Yes** | **Yes** | `src/batch_loader.py` (5 Batches @ 1000 rows/batch) |
| **9 Quality Rules (Tested)** | **Yes** | **Yes** | `src/quality_rules.py` / [`13_quality_rules_proof.png`](screenshots/13_quality_rules_proof.png) |
| **PyTest Suite (100% Pass)** | **Yes** | **Yes** | `pytest tests/` (33 Passed, 1 Skipped) |
| **Python vs Spark Benchmark** | **Yes** | **Yes** | جدول المقارنة الشامل في `reports/results.md` |
| **Spark Standalone Master** | **Yes** | **Yes** | `spark://127.0.0.1:7077` |
| **Worker ALIVE** | **Yes** | **Yes** | [`01_master_worker_alive.png`](screenshots/01_master_worker_alive.png) |
| **No Local Fallback Guard** | **Yes** | **Yes** | `PIPELINE_DISABLE_SPARK_FALLBACK=true` |
| **1,000,000 Rows Processing** | **Yes** | **Yes** | `data/orders_1m_sample.csv` (418.55 MB) |
| **Executors, Jobs, Stages, Tasks** | **Yes** | **Yes** | [`02_spark_application.png`](screenshots/02_spark_application.png), [`03_executors.png`](screenshots/03_executors.png), [`04_jobs_stages_tasks.png`](screenshots/04_jobs_stages_tasks.png) |
| **8 Partitions & explain(True)** | **Yes** | **Yes** | `repartition(8)` / [`05_repartition_explain.png`](screenshots/05_repartition_explain.png) |
| **Raw Load to orders_raw** | **Yes** | **Yes** | `orders_raw` in MongoDB / [`06_mongodb_raw.png`](screenshots/06_mongodb_raw.png) |
| **Quality Classification & ELT** | **Yes** | **Yes** | `orders_validated` / [`07_mongodb_validated.png`](screenshots/07_mongodb_validated.png) |
| **Quarantine Isolation** | **Yes** | **Yes** | `orders_quarantine` / [`08_mongodb_quarantine.png`](screenshots/08_mongodb_quarantine.png) |
| **Audit Trail (corrections)** | **Yes** | **Yes** | [`07_mongodb_validated.png`](screenshots/07_mongodb_validated.png) |
| **Upsert & Idempotency Proof** | **Yes** | **Yes** | [`09_idempotency_run1.png`](screenshots/09_idempotency_run1.png), [`10_idempotency_run2.png`](screenshots/10_idempotency_run2.png) |
| **In-Place Update Proof** | **Yes** | **Yes** | [`11_update_evidence.png`](screenshots/11_update_evidence.png) |
| **Results Logging** | **Yes** | **Yes** | `reports/results.json` & `reports/results.md` |
| **All Screenshots Generated** | **Yes** | **Yes** | `reports/screenshots/*.png` (13 ملفاً حقيقياً) |
