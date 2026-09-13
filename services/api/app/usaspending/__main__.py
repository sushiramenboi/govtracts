from __future__ import annotations

import argparse
from datetime import date

from app.core.config import load_settings
from app.db.session import Database
from app.usaspending.ingestion import UsaSpendingIngestion


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest USAspending Cyber/IT award data.")
    parser.add_argument("--start-date", type=date.fromisoformat, required=True)
    parser.add_argument("--end-date", type=date.fromisoformat, required=True)
    parser.add_argument("--page-size", type=int, default=100)
    parser.add_argument("--max-pages", type=int)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = load_settings()
    database = Database(settings)
    try:
        run_id = UsaSpendingIngestion(database.engine, settings).run(args.start_date, args.end_date, args.page_size, args.max_pages)
        print(f"USAspending ingestion succeeded: run_id={run_id}")
    finally:
        database.dispose()


if __name__ == "__main__":
    main()
