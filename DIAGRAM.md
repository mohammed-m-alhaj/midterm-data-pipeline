# 📊 Data Pipeline Architecture & Complete Workflow Diagrams

هذا الملف يوضح المعمارية الكاملة ومخططات تدفق ومعالجة البيانات الخاصة بمشروع **Enterprise Hybrid Data Pipeline**، مع تفصيل كامل لكافة المحركات وقواعد الجودة ومعمارية الكلاستر ودورة حياة اللاتكرارية:

---

## 1. المخطط العام لمراحل خط البيانات (End-to-End Pipeline Architecture)

```mermaid
flowchart TD
    subgraph S1 [" 1. استلام وتوجيه الملف (Ingestion & Routing) "]
        A["📄 ملف CSV خام<br/><b>(Dirty Orders Dataset)</b>"] --> B{"🔀 موجه الملفات<br/><b>src/file_router.py</b><br/>حجم الملف <= 200 MB؟"}
        B -- "نعم (حجم صغير)" --> C["⚡ تحميل تدفقي بالبايثون<br/><b>src/batch_loader.py</b><br/>• Streaming csv.DictReader<br/>• Batched insert_many"]
        B -- "لا (حجم ضخم)" --> D["🚀 معالجة متوازية بـ Spark<br/><b>src/spark_loader.py</b><br/>• Fixed Schema & Repartition<br/>• Mongo Spark Connector"]
    end

    subgraph S2 [" 2. طبقة التخزين الخام (Raw Storage Layer) "]
        C --> E[("🥉 MongoDB: orders_raw<br/><b>(حفظ كامل للسجلات الأصلية)</b><br/>• run_id & source_row_number<br/>• raw_record (Original JSON)<br/>• ingested_at & engine_used")]
        D --> E
    end

    subgraph S3 [" 3. محرك التحويل وتطبيق قواعد الجودة (ELT & Quality Rules Engine) "]
        E --> F["⚙️ محرك التحويل الموزع<br/><b>src/elt_pipeline.py</b><br/>قراءة الدفعة عبر run_id في PySpark"]
        F --> G["🧹 9 قواعد تنظيف وتطبيع حتمية<br/><b>src/quality_rules.py</b><br/>• تحويل الأرقام العربية إلى إنجليزية<br/>• توحيد العملة إلى YER وإزالة النصوص<br/>• تنظيف فواصل الآلاف والأرقام النصية<br/>• توحيد أرقام الهواتف (+9677XXXXXXXX)<br/>• إصلاح الرموز المكررة في البريد الإلكتروني<br/>• توحيد التواريخ إلى ISO Timestamp<br/>• إعادة احتساب إجمالي الطلب ومطابقته"]
    end

    subgraph S4 [" 4. فحص الجودة والتصنيف (Quality Classification & Hashing) "]
        G --> H{"🔍 هل يحتوي السجل<br/>على أخطاء غير قابلة للإصلاح؟"}
        H -- "سليم أو مصحح بأمان" --> I["✅ إنشاء سجل تدقيق (Audit Trail)<br/>• quality_status = valid / corrected<br/>• corrections [field, old, new, rule]<br/>• حساب record_hash (SHA-256)"]
        H -- "خطأ جسيم أو غير قابل للإصلاح" --> J["⚠️ تصنيف للعزل (Quarantine)<br/>• quality_status = quarantine<br/>• error_codes [MISSING_ID, CORRUPT_JSON...]<br/>• error_details"]
    end

    subgraph S5 [" 5. طبقات التخزين النهائية (Final Storage Layers) "]
        I --> K[("🥇 MongoDB: orders_validated<br/><b>(Idempotent Upsert)</b><br/>• مفتاح فريد: uq_validated_order_id<br/>• عملية Replace/Upsert على order_id<br/>• إحصاء: inserted / updated / unchanged")]
        J --> L[("🛡️ MongoDB: orders_quarantine<br/><b>(سجلات معزولة للدراسة والتحليل)</b><br/>• عزل الأخطاء دون إسقاطها")]
    end

    subgraph S6 [" 6. المراقبة والمقاييس (Observability & Metrics) "]
        K --> M["📊 حفظ مقاييس الأداء<br/><b>reports/results.json</b><br/>• الزمن، معدل التدفق (rows/s)<br/>• عدد الإدخالات والتحديثات<br/>• التحقق من معادلة الاتساق"]
        L --> M
    end

    classDef sourceStyle fill:#1e293b,stroke:#38bdf8,stroke-width:2px,color:#fff;
    classDef routerStyle fill:#0f172a,stroke:#f59e0b,stroke-width:2px,color:#fff;
    classDef processStyle fill:#047857,stroke:#10b981,stroke-width:2px,color:#fff;
    classDef rawStyle fill:#78350f,stroke:#d97706,stroke-width:2px,color:#fff;
    classDef validStyle fill:#14532d,stroke:#22c55e,stroke-width:2px,color:#fff;
    classDef quarStyle fill:#7f1d1d,stroke:#ef4444,stroke-width:2px,color:#fff;
    classDef metricsStyle fill:#312e81,stroke:#6366f1,stroke-width:2px,color:#fff;

    class A sourceStyle;
    class B routerStyle;
    class C,D,F,G processStyle;
    class E rawStyle;
    class I,K validStyle;
    class J,L quarStyle;
    class M metricsStyle;
```

---

## 2. دياجرام تفصيلي لمحرك التحويل الموزع (`src/elt_pipeline.py`)

```mermaid
flowchart TD
    subgraph P1 [" 1. استرجاع وتفكيك الدفعة الخام (Ingest & Parse) "]
        IN[("MongoDB: orders_raw")] -->|"$match: {run_id: run_id}"| SP["قراءة PySpark DataFrame"]
        SP --> PARSE_REC["from_json(raw_record, RAW_SCHEMA)<br/>استخراج 17 حقلاً أصلياً"]
        PARSE_REC --> PARSE_ITEMS["from_json(items_json, ITEM_SCHEMA)<br/>تحويل JSON عناصر الطلب لمصفوفة كائنات"]
    end

    subgraph P2 [" 2. تطبيق التطبيع والتحويلات الحتمية (Field Standardizations) "]
        PARSE_ITEMS --> N_CURR["تنظيف العملة:<br/>YER / ريال / ر.ي ➔ YER"]
        PARSE_ITEMS --> N_PHONE["تطبيع الهاتف:<br/>+9677XXXXXXXX"]
        PARSE_ITEMS --> N_EMAIL["إصلاح البريد:<br/>إزالة @ المتكررة والنقاط"]
        PARSE_ITEMS --> N_DATE["توحيد التاريخ:<br/>to_timestamp ➔ ISO Format"]
        PARSE_ITEMS --> N_MONEY["معالجة المبالغ المالية:<br/>money_expr(delivery, payment, total)"]
        PARSE_ITEMS --> N_ENUM["توحيد الحالات:<br/>مدفوع ➔ تم الدفع"]
        PARSE_ITEMS --> N_TEXT["تنظيف المسافات:<br/>trim & whitespace collapse"]
    end

    subgraph P3 [" 3. الفحص الشامل واكتشاف الأخطاء (Error Detection & Integrity Checks) "]
        N_CURR & N_PHONE & N_EMAIL & N_DATE & N_MONEY & N_ENUM & N_TEXT --> CHK_DUP["فحص التكرار الداخلي:<br/>groupBy(order_id).count() > 1 ➔ DUPLICATE_ORDER_ID"]
        CHK_DUP --> CHK_ITEMS["فحص بنية المنتجات:<br/>• CORRUPTED_ITEMS_JSON<br/>• EMPTY_ITEMS<br/>• UNKNOWN_PRICE<br/>• AMBIGUOUS_NEGATIVE_VALUE"]
        CHK_ITEMS --> CHK_KEYS["فحص الحقول الإلزامية والصلاحية:<br/>• MISSING_ORDER_ID<br/>• MISSING_CUSTOMER_ID<br/>• INVALID_IMPOSSIBLE_DATE<br/>• INVALID_EMAIL / INVALID_PHONE"]
        CHK_KEYS --> CALC_TOTAL["إعادة احتساب الإجمالي:<br/>Total = sum(unit_price * qty) + delivery_cost"]
    end

    subgraph P4 [" 4. بناء سجل التدقيق والتصنيف (Audit Trail & Classification) "]
        CALC_TOTAL --> BUILD_ERR["تجميع الأخطاء في مصفوفة error_codes<br/>(إذا > 1 خطأ ➔ إضافة MULTIPLE_CONFLICTING_ERRORS)"]
        BUILD_ERR --> BUILD_CORR["بناء مصفوفة سجل التدقيق corrections<br/>[field, original_value, corrected_value, rule_code]"]
        BUILD_CORR --> CALC_HASH["توليد التجزئة SHA-256<br/>record_hash لجميع الحقول المنظفة"]
        CALC_HASH --> DECIDE{"تصنيف السجل<br/>quality_status"}
    end

    subgraph P5 [" 5. توجيه المخرجات والحفظ الذكي (Atomic Upsert & Isolation) "]
        DECIDE -- "size(error_codes) == 0<br/>(valid / corrected)" --> D_VALID["Orders Validated DataFrame"]
        DECIDE -- "size(error_codes) > 0<br/>(quarantine)" --> D_QUAR["Orders Quarantine DataFrame"]

        D_VALID --> HASH_DIFF["مقارنة record_hash مع قاعدة البيانات:<br/>• inserted_count (سجل جديد)<br/>• updated_count (سجل موجود مع تغير الهاش)<br/>• unchanged_count (مطابق تماماً)"]
        HASH_DIFF --> SAVE_VAL[("MongoDB: orders_validated<br/><b>Spark Upsert Write</b><br/>• idfieldlist: order_id<br/>• operationtype: replace<br/>• upsertdocument: true")]

        D_QUAR --> SAVE_QUAR[("MongoDB: orders_quarantine<br/><b>Quarantine Write</b><br/>حفظ مع error_codes و error_details")]
    end

    subgraph P6 [" 6. التحقق النهائي والمقاييس (Consistency & Metrics) "]
        SAVE_VAL & SAVE_QUAR --> ASSERT_EQ{"فحص معادلة الاتساق:<br/>raw == valid + corrected + quarantine"}
        ASSERT_EQ -- "True" --> SAVE_METRICS["تحديث reports/results.json<br/>(Throughput, Counts, Error Breakdown)"]
        ASSERT_EQ -- "False" --> ERR_RAISE["إيقاف الخط وتوليد خطأ اتساق"]
    end

    classDef startBlock fill:#0284c7,stroke:#38bdf8,stroke-width:2px,color:#fff;
    classDef transformBlock fill:#0f766e,stroke:#2dd4bf,stroke-width:2px,color:#fff;
    classDef checkBlock fill:#854d0e,stroke:#facc15,stroke-width:2px,color:#fff;
    classDef decisionBlock fill:#6b21a8,stroke:#c084fc,stroke-width:2px,color:#fff;
    classDef validBlock fill:#166534,stroke:#4ade80,stroke-width:2px,color:#fff;
    classDef quarBlock fill:#991b1b,stroke:#f87171,stroke-width:2px,color:#fff;

    class IN,SP,PARSE_REC,PARSE_ITEMS startBlock;
    class N_CURR,N_PHONE,N_EMAIL,N_DATE,N_MONEY,N_ENUM,N_TEXT transformBlock;
    class CHK_DUP,CHK_ITEMS,CHK_KEYS,CALC_TOTAL,BUILD_ERR checkBlock;
    class DECIDE,ASSERT_EQ decisionBlock;
    class D_VALID,HASH_DIFF,SAVE_VAL validBlock;
    class D_QUAR,SAVE_QUAR,ERR_RAISE quarBlock;
```

---

## 3. دياجرام تفصيلي لقواعد الجودة والتنظيف (`src/quality_rules.py`)

```mermaid
flowchart LR
    subgraph R1 [" 1. معالجة وتطبيع المبالغ والأرقام (normalize_number_text & to_decimal) "]
        IN1["نص القيمة المدخلة"] --> TR1["تحويل الأرقام العربية المشرقية<br/>٠١٢٣٤٥٦٧٨٩ ➔ 0123456789"]
        TR1 --> CLN1["تنظيف الفواصل العشرية وفواصل الآلاف<br/>'٫' ➔ '.' وإزالة ',' و '٬'"]
        CLN1 --> REM1["إزالة نصوص العملة عبر Regex<br/>(ريال يمني | ريال | ر.ي | YER)"]
        REM1 --> WORD1{"هل القيمة نصية بكلمات؟<br/>(ألفان / خمسة آلاف / عشرة آلاف)"}
        WORD1 -- "نعم" --> MAP1["تحويل إلى القيمة الرقمية:<br/>ألفان ➔ 2000<br/>خمسة آلاف ➔ 5000<br/>عشرة آلاف ➔ 10000"]
        WORD1 -- "لا" --> DEC1["تحويل النص إلى Decimal / Double"]
        MAP1 --> DEC1
    end

    subgraph R2 [" 2. تطبيع أرقام الهواتف اليمنية (normalize_phone) "]
        IN2["رقم الهاتف المدخل"] --> STRIP2["استخراج الأرقام فقط وتجاهل الرموز"]
        STRIP2 --> PREFIX2{"فحص البادئة"}
        PREFIX2 -- "تبدأ بـ 00967 أو 967" --> PR1["استخراج الرقم الوطني (9 خانات تبدأ بـ 7)"]
        PREFIX2 -- "تبدأ بـ 07 أو 7 (9 خانات)" --> PR2["إضافة المفتاح الدولي +967"]
        PR1 --> OUT2["الصيغة القياسية: +9677XXXXXXXX"]
        PR2 --> OUT2
        PREFIX2 -- "غير ذلك (طول خاطئ / لا تبدأ بـ 7)" --> ERR_PH["إرجاع None ➔ وسم كـ INVALID_PHONE"]
    end

    subgraph R3 [" 3. إصلاح وفحص البريد الإلكتروني (normalize_email & is_valid_email) "]
        IN3["البريد الإلكتروني"] --> REP_AT["إصلاح الرموز المكررة:<br/>@+ ➔ @<br/>\\.{2,} ➔ ."]
        REP_AT --> LOW3["تحويل إلى أحرف صغيرة (lowercase)"]
        LOW3 --> CHK_EMAIL{"مطابقة Regex:<br/>^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$"}
        CHK_EMAIL -- "صحيح" --> OUT3["بريد سليم + توثيق التغيير في corrections"]
        CHK_EMAIL -- "غير مطابق" --> ERR_EM["إرجاع غير صالح ➔ وسم كـ INVALID_EMAIL"]
    end

    subgraph R4 [" 4. توحيد الحالات والمرادفات (standardize_status) "]
        IN4["نص الحالة المدخلة"] --> MAP_ST{"مطابقة المرادفات"}
        MAP_ST -- "مدفوع / دفع" --> S1["تم الدفع"]
        MAP_ST -- "غير مدفوع" --> S2["بانتظار الدفع"]
        MAP_ST -- "مأكد" --> S3["مؤكد"]
        MAP_ST -- "أخرى" --> S4["الحالة المنظفة (Trimmed)"]
    end

    classDef r1Style fill:#1e3a8a,stroke:#60a5fa,stroke-width:2px,color:#fff;
    classDef r2Style fill:#14532d,stroke:#4ade80,stroke-width:2px,color:#fff;
    classDef r3Style fill:#701a75,stroke:#f472b6,stroke-width:2px,color:#fff;
    classDef r4Style fill:#7c2d12,stroke:#fb923c,stroke-width:2px,color:#fff;

    class IN1,TR1,CLN1,REM1,WORD1,MAP1,DEC1 r1Style;
    class IN2,STRIP2,PREFIX2,PR1,PR2,OUT2,ERR_PH r2Style;
    class IN3,REP_AT,LOW3,CHK_EMAIL,OUT3,ERR_EM r3Style;
    class IN4,MAP_ST,S1,S2,S3,S4 r4Style;
```

---

## 4. مخطط دورة حياة ومراحل اللاتكرارية والتحديث (Idempotency & SHA-256 State Machine)

```mermaid
flowchart TD
    subgraph IDEMP [" دورة حياة معالجة السجل وضمان عدم التكرار "]
        R_IN["سجل مصحح في PySpark DataFrame"] --> GEN_HASH["حساب تجزئة التشفير SHA-256:<br/>record_hash = SHA256(order_id || date || status || phone || total || ...)"]
        GEN_HASH --> MONGO_MATCH{"هل السجل order_id<br/>موجود في MongoDB؟"}
        
        MONGO_MATCH -- "سجل جديد تماماً (Not Found)" --> DO_INSERT["➕ عملية إدخال جديدة (New Document)<br/>• يُكتب في orders_validated<br/>• يُحسب كـ inserted_count"]
        
        MONGO_MATCH -- "موجود مسبقاً (Found)" --> COMP_HASH{"مقارنة record_hash الحالي<br/>مع record_hash المخزن"}
        
        COMP_HASH -- "الهاش مختلف (بيانات تم تعديلها)" --> DO_UPDATE["🔄 تحديث في نفس المكان (In-Place Mutation)<br/>• استبدال الوثيقة عبر Upsert (replace)<br/>• يُحسب كـ updated_count<br/>• لا يزداد إجمالي عدد الوثائق"]
        
        COMP_HASH -- "الهاش متطابق (مطابق 100%)" --> DO_UNCHANGED["⏭️ تجاهل آمن دون تكرار (Idempotent No-Op)<br/>• الحفاظ على الوثيقة الحالية<br/>• يُحسب كـ unchanged_count<br/>• inserted_count = 0"]
    end

    classDef hashBlock fill:#1e1b4b,stroke:#818cf8,stroke-width:2px,color:#fff;
    classDef decBlock fill:#4c1d95,stroke:#a78bfa,stroke-width:2px,color:#fff;
    classDef insBlock fill:#064e3b,stroke:#34d399,stroke-width:2px,color:#fff;
    classDef updBlock fill:#78350f,stroke:#fbbf24,stroke-width:2px,color:#fff;
    classDef uncBlock fill:#1e293b,stroke:#94a3b8,stroke-width:2px,color:#fff;

    class R_IN,GEN_HASH hashBlock;
    class MONGO_MATCH,COMP_HASH decBlock;
    class DO_INSERT insBlock;
    class DO_UPDATE updBlock;
    class DO_UNCHANGED uncBlock;
```

---

## 5. معمارية الكلاستر والمسار الموزع (Path A: Spark Standalone Architecture)

```mermaid
flowchart LR
    subgraph CLUSTER [" بيئة المعالجة الموزعة (Path A: Spark Standalone Cluster) "]
        subgraph DRIVER_NODE [" 🖥️ Driver Process "]
            CLI["spark-submit / Python CLI"]
            APP["PySpark Application (Master Coordinator)<br/>Driver Memory: 6GB"]
            SPK_UI["Spark Application UI<br/>http://127.0.0.1:4040"]
            CLI --> APP
            APP -.-> SPK_UI
        end

        subgraph MASTER_NODE [" 🌐 Spark Master Node "]
            MSTR["Spark Master Daemon<br/>spark://127.0.0.1:7077"]
            MST_UI["Spark Master Web UI<br/>http://127.0.0.1:8080"]
            MSTR -.-> MST_UI
        end

        subgraph WORKER_NODE [" ⚙️ Spark Worker Node "]
            WRK["Worker Daemon (Registered ALIVE)"]
            EX1["Executor 1 (8 CPU Cores + RAM)"]
            EX2["Executor 2 (Tasks Pool)"]
            WRK --> EX1 & EX2
        end

        subgraph DB_NODE [" 🗄️ NoSQL Storage Node "]
            MDB[("MongoDB Instance :27017<br/>midterm_pipeline Database")]
        end

        APP -->|تسجيل الوظيفة وتوزيع المهام| MSTR
        MSTR -->|تخصيص الموارد والمنفذين| WRK
        EX1 & EX2 -->|قراءة متوازية وكتابة دفعات| MDB
    end

    classDef driverStyle fill:#0f172a,stroke:#38bdf8,stroke-width:2px,color:#fff;
    classDef masterStyle fill:#1e1b4b,stroke:#818cf8,stroke-width:2px,color:#fff;
    classDef workerStyle fill:#064e3b,stroke:#34d399,stroke-width:2px,color:#fff;
    classDef dbStyle fill:#701a75,stroke:#f472b6,stroke-width:2px,color:#fff;

    class CLI,APP,SPK_UI driverStyle;
    class MSTR,MST_UI masterStyle;
    class WRK,EX1,EX2 workerStyle;
    class MDB dbStyle;
```

---

## 6. شجرة قرارات تصنيف وعزل الأخطاء (Quarantine Decision Tree & Error Hierarchy)

```mermaid
flowchart TD
    subgraph QUAR_TREE [" آلية اكتشاف وتصنيف الأخطاء غير القابلة للإصلاح "]
        RAW_DOC["السجل المفحوص"] --> EVAL_ERRS{"فحص 8 شروط للأخطاء الجسيمة"}
        
        EVAL_ERRS --> E1["MISSING_ORDER_ID<br/>(معرف الطلب فارغ)"]
        EVAL_ERRS --> E2["MISSING_CUSTOMER_ID<br/>(معرف العميل فارغ)"]
        EVAL_ERRS --> E3["INVALID_IMPOSSIBLE_DATE<br/>(تاريخ غير منطقي مثل 31 فبراير)"]
        EVAL_ERRS --> E4["CORRUPTED_ITEMS_JSON<br/>(نص JSON مكسور أو غير صالح)"]
        EVAL_ERRS --> E5["EMPTY_ITEMS<br/>(مصفوفة منتجات فارغة)"]
        EVAL_ERRS --> E6["UNKNOWN_PRICE<br/>(سعر مجهول ولا يمكن استنتاجه)"]
        EVAL_ERRS --> E7["AMBIGUOUS_NEGATIVE_VALUE<br/>(كمية أو سعر سالب غير منطقي)"]
        EVAL_ERRS --> E8["DUPLICATE_ORDER_ID<br/>(معرف مكرر داخل نفس الدفعة)"]

        E1 & E2 & E3 & E4 & E5 & E6 & E7 & E8 --> AGG_ERR["تجميع الأخطاء في مصفوفة error_codes"]
        AGG_ERR --> CHK_COUNT{"كم عدد الأخطاء المكتشفة؟"}

        CHK_COUNT -- "= 0 خطأ" --> STAT_OK["✅ quality_status = 'valid' أو 'corrected'"]
        CHK_COUNT -- "= 1 خطأ" --> STAT_Q1["⚠️ quality_status = 'quarantine'<br/>كتابة رمز الخطأ وسببه بالعربية"]
        CHK_COUNT -- "> 1 خطأ" --> STAT_QM["🚫 إضافة MULTIPLE_CONFLICTING_ERRORS<br/>quality_status = 'quarantine'"]

        STAT_Q1 & STAT_QM --> SAVE_Q[("MongoDB: orders_quarantine<br/>مع حفظ raw_record و error_details")]
    end

    classDef okStyle fill:#14532d,stroke:#22c55e,stroke-width:2px,color:#fff;
    classDef warnStyle fill:#7f1d1d,stroke:#ef4444,stroke-width:2px,color:#fff;
    classDef checkStyle fill:#1e293b,stroke:#f59e0b,stroke-width:2px,color:#fff;

    class RAW_DOC,EVAL_ERRS,AGG_ERR,CHK_COUNT checkStyle;
    class STAT_OK okStyle;
    class E1,E2,E3,E4,E5,E6,E7,E8,STAT_Q1,STAT_QM,SAVE_Q warnStyle;
```

---

## 7. مخطط الكلاسات والموديولات الشامل (Class & Module Diagram)

```mermaid
classDiagram
    class Settings {
        +int SMALL_FILE_THRESHOLD_MB
        +int BATCH_SIZE
        +int SPARK_PARTITIONS
        +string MONGO_URI
        +string MONGO_DATABASE
        +string RAW_COLLECTION
        +string VALIDATED_COLLECTION
        +string QUARANTINE_COLLECTION
        +list RAW_COLUMNS
        +ensure_directories() void
    }

    class FileRouter {
        +route_file(file_path: str) dict
    }

    class MainPipeline {
        +parse_args() Namespace
        +main() void
    }

    class BatchLoader {
        +load_csv_to_raw(file_path, run_id, engine_used, batch_size) dict
    }

    class SparkLoader {
        +create_spark() SparkSession
        +build_raw_schema() StructType
        +resolve_safe_spark_path(file_path) str
        +load_csv_to_raw(file_path, run_id, engine_used, partitions) dict
    }

    class QualityRules {
        +dict ARABIC_DIGITS
        +dict KNOWN_WORD_NUMBERS
        +set ERROR_CODES
        +normalize_number_text(val: Any) str
        +to_decimal(val: Any) Decimal
        +normalize_phone(val: Any) str
        +normalize_email(val: Any) tuple[str, bool]
        +is_valid_email(val: Any) bool
        +standardize_text(val: Any) str
        +standardize_status(val: Any) str
        +classify_errors(errors: list) str
    }

    class ELTPipeline {
        +StructType RAW_SCHEMA
        +ArrayType ITEM_SCHEMA
        +StructType CORRECTION_STRUCT
        +money_expr(col: str) Column
        +standardize_phone_expr(col: str) Column
        +standardize_enum_expr(col: str) Column
        +correction(field, orig, clean, code) Column
        +non_null_array(items: list) Column
        +error_array(conditions: list) Column
        +process_run(run_id: str, source_file: str) dict
        +get_latest_run_id() str
    }

    class MongoSetup {
        +dict VALIDATED_SCHEMA
        +setup_mongodb() void
    }

    class MetricsTracker {
        +Path RESULTS_FILE
        +append_run_metrics(metrics: dict) void
        +read_metrics() list[dict]
    }

    MainPipeline --> FileRouter : يستدعي للتوجيه
    MainPipeline --> MongoSetup : يضمن تهيئة المجموعات
    MainPipeline --> BatchLoader : يستدعي إذا كان الملف صغير
    MainPipeline --> SparkLoader : يستدعي إذا كان الملف كبير
    MainPipeline --> ELTPipeline : يستدعي لتنفيذ التنظيف
    MainPipeline --> MetricsTracker : يحفظ مؤشرات الأداء

    ELTPipeline --> QualityRules : يطبق قواعد التنظيف
    ELTPipeline --> SparkLoader : يستخدم create_spark()
    ELTPipeline --> MetricsTracker : يرسل إحصائيات الدفعة
    BatchLoader --> Settings : يقرأ الإعدادات
    SparkLoader --> Settings : يقرأ الإعدادات
    ELTPipeline --> Settings : يقرأ الإعدادات
```

---

## 8. مخطط تسلسل تنفيذ خط البيانات (Pipeline Sequence Diagram)

```mermaid
sequenceDiagram
    autonumber
    actor User as المستخدم / Terminal
    participant Main as src/main.py
    participant Router as src/file_router.py
    participant DB as MongoDB (Engine)
    participant Batch as src/batch_loader.py
    participant Spark as src/spark_loader.py
    participant ELT as src/elt_pipeline.py
    participant QR as src/quality_rules.py
    participant Metrics as reports/results.json

    User->>Main: python src/main.py --file input.csv
    Main->>Router: route_file(input.csv)
    Router-->>Main: {run_id, engine: python_batch | pyspark, size_mb}

    alt حجم الملف <= 200 MB (Python Batch)
        Main->>Batch: load_csv_to_raw(file, run_id)
        Batch->>DB: insert_many -> orders_raw (Chunks)
        Batch-->>Main: {rows_read, raw_loaded, throughput}
    else حجم الملف > 200 MB (PySpark)
        Main->>Spark: load_csv_to_raw(file, run_id)
        Spark->>DB: Parallel Write -> orders_raw
        Spark-->>Main: {rows_read, partitions, throughput}
    end

    Main->>ELT: process_run(run_id)
    ELT->>DB: Read orders_raw where run_id = run_id
    ELT->>QR: استدعاء قواعد التطبيع والتعابير النمطية
    QR-->>ELT: دوال money_expr, standardize_phone, etc.
    ELT->>ELT: 1. تنظيف وتطبيع البيانات (9 Rules)<br/>2. بناء سجل التدقيق (corrections)<br/>3. حساب SHA-256 record_hash<br/>4. فرز وتصنيف الأخطاء (Classification)

    par حفظ السجلات المصححة والسليمة
        ELT->>DB: Upsert (replace) -> orders_validated on order_id
    and حفظ السجلات المعزولة
        ELT->>DB: Append -> orders_quarantine
    end

    ELT->>ELT: فحص معادلة الاتساق: raw == valid + corrected + quarantine
    ELT->>Metrics: حفظ مقاييس الدفعة (results.json)
    ELT-->>Main: اكتملت المعالجة بنجاح
    Main-->>User: عرض تقرير الإحصائيات والإنجاز في Terminal
```

---

## 9. مخطط بنية المجموعات وقواعد التحقق (MongoDB Collections & Data Model)

```mermaid
erDiagram
    ORDERS_RAW {
        string run_id PK "معرف الدفعة"
        string source_file "مسار الملف المصدر"
        long source_row_number "رقم السطر في CSV"
        timestamp ingested_at "وقت التحميل الأولي"
        string engine_used "المحرك (python_batch / pyspark)"
        string raw_record "السجل الأصلي JSON بدون تعديل"
    }

    ORDERS_VALIDATED {
        string order_id PK "المفتاح الفريد المستقر"
        string order_date "تاريخ الطلب الموحد ISO"
        string status "حالة الطلب القياسية"
        string customer_id "معرف العميل"
        string customer_name "اسم العميل المنظف"
        string customer_phone "رقم الهاتف الموحد +9677XXXXXXXX"
        string customer_email "البريد الإلكتروني بعد الإصلاح"
        double delivery_cost "تكلفة التوصيل كرقم عشري"
        double total_amount "المبلغ الإجمالي بعد إعادة الحساب"
        string currency "العملة الموحدة YER"
        string quality_status "الحالة (valid / corrected)"
        string record_hash "تجزئة SHA-256 لجميع الحقول"
        array corrections "سجل تفاصيل التعديلات السابقة"
    }

    ORDERS_QUARANTINE {
        string run_id "معرف الدفعة"
        string order_id "معرف الطلب (إن وجد)"
        array error_codes "أكواد الأخطاء الجسيمة"
        string error_details "نص تفاصيل الأسباب"
        string quality_status "quarantine دائماً"
        string raw_record "السجل الخام المعزول"
    }

    ORDERS_RAW ||--o{ ORDERS_VALIDATED : "تحويل وتصنيف"
    ORDERS_RAW ||--o{ ORDERS_QUARANTINE : "عزل الأخطاء غير القابلة للتصحيح"
```
