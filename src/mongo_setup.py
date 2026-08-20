from __future__ import annotations

from common import PROJECT_ROOT  # noqa: F401

from pymongo import ASCENDING, MongoClient

from config.settings import (
    MONGO_DATABASE,
    MONGO_URI,
    QUARANTINE_COLLECTION,
    RAW_COLLECTION,
    VALIDATED_COLLECTION,
)


VALIDATED_SCHEMA = {
    "bsonType": "object",
    "required": [
        "order_id",
        "order_date",
        "customer_id",
        "items_json",
        "quality_status",
        "record_hash",
    ],
    "properties": {
        "order_id": {"bsonType": "string"},
        "order_date": {"bsonType": ["string", "null"]},
        "status": {"bsonType": ["string", "null"]},
        "customer_id": {"bsonType": "string"},
        "customer_name": {"bsonType": ["string", "null"]},
        "customer_phone": {"bsonType": ["string", "null"]},
        "customer_email": {"bsonType": ["string", "null"]},
        "currency": {"enum": ["YER", None]},
        "quality_status": {"enum": ["valid", "corrected"]},
        "record_hash": {"bsonType": "string"},
        "corrections": {"bsonType": "array"},
    },
}


def setup_mongodb() -> None:
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    try:
        client.admin.command("ping")
        db = client[MONGO_DATABASE]
        existing = set(db.list_collection_names())

        for name in (RAW_COLLECTION, QUARANTINE_COLLECTION):
            if name not in existing:
                db.create_collection(name)

        if VALIDATED_COLLECTION not in existing:
            db.create_collection(
                VALIDATED_COLLECTION,
                validator={"$jsonSchema": VALIDATED_SCHEMA},
                validationLevel="strict",
                validationAction="error",
            )
        else:
            db.command(
                "collMod",
                VALIDATED_COLLECTION,
                validator={"$jsonSchema": VALIDATED_SCHEMA},
                validationLevel="strict",
                validationAction="error",
            )

        validated = db[VALIDATED_COLLECTION]
        quarantine = db[QUARANTINE_COLLECTION]

        validated.create_index(
            [("order_id", ASCENDING)],
            unique=True,
            name="uq_validated_order_id",
        )
        validated.create_index([("quality_status", ASCENDING)], name="idx_validated_quality_status")
        validated.create_index([("run_id", ASCENDING)], name="idx_validated_run_id")
        validated.create_index([("record_hash", ASCENDING)], name="idx_validated_record_hash")

        quarantine.create_index([("order_id", ASCENDING)], name="idx_quarantine_order_id")
        quarantine.create_index([("error_codes", ASCENDING)], name="idx_quarantine_error_codes")
        quarantine.create_index([("run_id", ASCENDING)], name="idx_quarantine_run_id")

        print("=" * 60)
        print("MONGODB SETUP")
        print("=" * 60)
        print(f"Database              : {MONGO_DATABASE}")
        print(f"Raw collection        : {RAW_COLLECTION}")
        print(f"Validated collection  : {VALIDATED_COLLECTION}")
        print(f"Quarantine collection : {QUARANTINE_COLLECTION}")
        print("Unique validated key  : order_id")
        print("=" * 60)
    finally:
        client.close()


if __name__ == "__main__":
    setup_mongodb()
