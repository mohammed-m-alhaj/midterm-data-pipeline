# 🎬 Official Live Defense & Presentation Checklist (Phase 1 & Phase 2)
### *جامعة الرازي — كلية الحاسوب وتقنية المعلومات — مقرر البيانات الضخمة (القسم العملي)*

---

## 📌 دليل خطوات المناقشة والعرض الحي الشامل (25.0 / 25.0 درجة)

يمكن تنفيذ العرض التقديمي للمشروع أمام لجنة التقييم والأستاذ المشرف عبر اتباع الخطوات المعيارية التالية:

| # | مرحلة العرض والمناقشة | الإجراء المطلوب تنفيذه أمام اللجنة | الأمر أو الرابط المباشر |
|:---:|---|---|---|
| **1** | **بيئة العمل وقاعدة البيانات** | التأكد من اتصال MongoDB وفحص الجاهزية | `mongosh --eval "db.runCommand({ping:1})"` |
| **2** | **توجيه الملف الصغير (Phase 1)** | إثبات اختيار `python_batch` تلقائياً للملف $\le 200\text{ MB}$ بذاكرة $O(1)$ | `python src/main.py --file data/test_1_small_clean.csv` |
| **3** | **طبقة التخزين الخام (Phase 1)** | استعراض وثائق `orders_raw` والتأكد من وجود سلالة البيانات `run_id` | MongoDB `orders_raw` |
| **4** | **قواعد الجودة والتنظيف (Phase 1)** | استعراض السجلات المصححة مع مصفوفة التدقيق `corrections` | MongoDB `orders_validated` |
| **5** | **طبقة العزل (Phase 1)** | استعراض سجلات `orders_quarantine` مع 13 رمز خطأ تشخيصي | MongoDB `orders_quarantine` |
| **6** | **إثبات اللاتكرارية (Phase 1)** | إعادة تشغيل نفس الملف وإثبات: `Inserted: 0`, `Unchanged: 4,213` | `python src/main.py --file data/test_1_small_clean.csv` |
| **7** | **توجيه الملف الكبير وعنقود Spark (Path A)** | إثبات اختيار `pyspark` وتوزيع البيانات على الـ Partitions | `python src/main.py --file data/orders_1m_sample.csv` أو `http://127.0.0.1:8080` |
| **8** | **حزمة الاختبارات الآلية (Phase 1 & 2)** | تشغيل PyTest وإثبات نجاح كافة الاختبارات الـ 23 بنسبة 100% | `python -m pytest` |
| **9** | **الفهارس وقاعدة ESR (Phase 2)** | إنشاء واستعراض الفهارس المركبة الأربعة المصممة بقاعدة ESR | `python -c "from src.queries import create_phase2_indexes; print(create_phase2_indexes())"` |
| **10** | **مقارنة Explain قبل وبعد (Phase 2)** | إثبات التحول من `COLLSCAN` إلى `IXSCAN` وخفض الوثائق المفحوصة بنسبة 99.8% | `python -c "from src.queries import run_all_queries; run_all_queries()"` |
| **11** | **تقارير التجميع الـ 6 (Phase 2)** | تشغيل تقارير التجميع واستعراض بيانات المدن والعملاء والشحن والمنتجات حية | `python -c "from src.aggregations import run_all_aggregations; print(run_all_aggregations())"` |
| **12** | **الجداول المجمعة والتحديث التزايدي (Phase 2)** | إثبات التحديث التزايدي بالعلامة المائية دون مسح الجدول القديم (32ms) | `python -c "from src.materialized_views import refresh_all_materialized_views; print(refresh_all_materialized_views(incremental=True))"` |
| **13** | **المهام المجدولة وسجلات التنفيذ (Phase 2)** | استعراض سجلات التنفيذ في `job_execution_logs` وتشغيل مهمة يدوياً | `python -c "from src.jobs import run_job_manually; print(run_job_manually('refresh_materialized_views'))"` |
| **14** | **خادم FastAPI و Swagger UI (Phase 2)** | تشغيل Uvicorn وتصفح Swagger التفاعلي واستعراض الـ 10 Endpoints | `uvicorn src.api:app --host 127.0.0.1 --port 8000` $\rightarrow$ `http://127.0.0.1:8000/docs` |
| **15** | **فحص POST /ingest الموحد (Phase 2)** | إرسال طلب ابتلاع لملف جديد عبر الـ API للتأكد من استدعاء Phase 1 مباشرة | عبر Swagger UI مسار `POST /ingest` |
| **16** | **فحص عدم الثباتية والبيانات الديناميكية** | إثبات عمل النظام ببيانات عشوائية جديدة دون أي نتائج ثابتة | `python src/test_different_dataset_live.py` |
