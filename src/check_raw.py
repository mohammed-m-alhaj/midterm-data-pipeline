from __future__ import annotations

from pymongo import MongoClient

from config.settings import MONGO_DATABASE, MONGO_URI, RAW_COLLECTION


def main() -> None:
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    try:
        client.admin.command("ping")
        col = client[MONGO_DATABASE][RAW_COLLECTION]
        total = col.count_documents({})
        sample = col.find_one({}, {"_id": 0})
        print("=" * 60)
        print("RAW DATA CHECK")
        print("=" * 60)
        print(f"Database   : {MONGO_DATABASE}")
        print(f"Collection : {RAW_COLLECTION}")
        print(f"Total rows : {total:,}")
        print("Sample:")
        print(sample or "EMPTY")
        print("=" * 60)
    finally:
        client.close()


if __name__ == "__main__":
    main()
