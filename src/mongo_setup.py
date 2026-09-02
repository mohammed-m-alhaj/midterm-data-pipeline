from __future__ import annotations

from bootstrap import ensure_project_root

ensure_project_root()

from pymongo import ASCENDING, MongoClient

from config.settings import (
    MONGO_DATABASE,
    MONGO_TIMEOUT_MS,
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
        "city": {"bsonType": ["string", "null"]},
        "district": {"bsonType": ["string", "null"]},
        "delivery_type": {"bsonType": ["string", "null"]},
        "delivery_cost": {"bsonType": ["double", "int", "long", "null"]},
        "payment_method": {"bsonType": ["string", "null"]},
        "payment_status": {"bsonType": ["string", "null"]},
        "payment_amount": {"bsonType": ["double", "int", "long", "null"]},
        "currency": {"enum": ["YER", None]},
        "total_amount": {"bsonType": ["double", "int", "long", "null"]},
        "items_json": {"bsonType": "string"},
        "quality_status": {"enum": ["valid", "corrected"]},
        "record_hash": {"bsonType": "string"},
        "corrections": {"bsonType": "array"},
    },
}


def setup_mongodb() -> None:
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=MONGO_TIMEOUT_MS)
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

        print("\033[96m" + "=" * 65 + "\033[0m")
        print("\033[1m\033[92mMONGODB SETUP & JSON SCHEMA VALIDATION CONFIRMATION\033[0m")
        print("\033[96m" + "=" * 65 + "\033[0m")
        print(f"\033[97mDatabase              :\033[0m \033[96m{MONGO_DATABASE}\033[0m")
        print(f"\033[97mRaw collection        :\033[0m \033[92m{RAW_COLLECTION}\033[0m")
        print(f"\033[97mValidated collection  :\033[0m \033[92m{VALIDATED_COLLECTION}\033[0m")
        print(f"\033[97mQuarantine collection :\033[0m \033[92m{QUARANTINE_COLLECTION}\033[0m")
        print(f"\033[97mJSON Schema Validation:\033[0m \033[1m\033[92mENFORCED ($jsonSchema strict mode)\033[0m")
        print(f"\033[97mSchema Required Fields:\033[0m \033[93morder_id, order_date, customer_id, items_json, quality_status, record_hash\033[0m")
        print(f"\033[97mAllowed Currencies    :\033[0m \033[95m['YER', null]\033[0m")
        print(f"\033[97mAllowed Statuses      :\033[0m \033[95m['valid', 'corrected']\033[0m")
        print(f"\033[97mUnique Validated Key  :\033[0m \033[1m\033[96muq_validated_order_id ON order_id (UNIQUE=TRUE)\033[0m")
        print("\033[96m" + "=" * 65 + "\033[0m\n")
    finally:
        client.close()


if __name__ == "__main__":
    setup_mongodb()
