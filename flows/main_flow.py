from datetime import datetime

from prefect import flow, get_run_logger

from db.database import init_db, SessionLocal
from db.models import SyncRun
from flows.extract.api_extractor import extract_from_api
from flows.extract.csv_extractor import extract_from_csv
from flows.extract.webhook_extractor import (
    extract_from_webhook,
    mark_webhook_events_processed,
)
from flows.load.loader import load_records
from flows.transform.transformer import transform_records
from models.schemas import SourceType

API_SOURCES = [
    {
        "url": "https://jsonplaceholder.typicode.com/posts",
        "name": "jsonplaceholder_posts",
    },
    {
        "url": "https://jsonplaceholder.typicode.com/users",
        "name": "jsonplaceholder_users",
    },
]

CSV_SOURCES = [
    {"path": "data/sample.csv", "name": "sample_csv"},
]


def _record_run(
    source_type: str,
    source_name: str,
    status: str,
    extracted: int = 0,
    loaded: int = 0,
    error: str = None,
    started_at: datetime = None,
) -> None:
    db = SessionLocal()
    try:
        db.add(
            SyncRun(
                source_type=source_type,
                source_name=source_name,
                status=status,
                records_extracted=extracted,
                records_loaded=loaded,
                error_message=error,
                started_at=started_at or datetime.utcnow(),
                finished_at=datetime.utcnow(),
            )
        )
        db.commit()
    finally:
        db.close()


@flow(name="data-sync-pipeline", log_prints=True)
def main_flow() -> int:
    logger = get_run_logger()
    logger.info("Initializing database schema")
    init_db()

    total = 0

    # ── API sources ────────────────────────────────────────────────────────
    for source in API_SOURCES:
        started = datetime.utcnow()
        try:
            raw = extract_from_api(source["url"], source["name"])
            normalized = transform_records(raw)
            count = load_records(normalized)
            _record_run(
                SourceType.API.value, source["name"], "success",
                extracted=len(raw), loaded=count, started_at=started,
            )
            total += count
            logger.info(f"[{source['name']}] {count} records loaded")
        except Exception as exc:
            logger.error(f"[{source['name']}] failed: {exc}")
            _record_run(
                SourceType.API.value, source["name"], "error",
                error=str(exc), started_at=started,
            )

    # ── CSV sources ────────────────────────────────────────────────────────
    for source in CSV_SOURCES:
        started = datetime.utcnow()
        try:
            raw = extract_from_csv(source["path"], source["name"])
            normalized = transform_records(raw)
            count = load_records(normalized)
            _record_run(
                SourceType.CSV.value, source["name"], "success",
                extracted=len(raw), loaded=count, started_at=started,
            )
            total += count
            logger.info(f"[{source['name']}] {count} records loaded")
        except Exception as exc:
            logger.error(f"[{source['name']}] failed: {exc}")
            _record_run(
                SourceType.CSV.value, source["name"], "error",
                error=str(exc), started_at=started,
            )

    # ── Webhook buffer ─────────────────────────────────────────────────────
    started = datetime.utcnow()
    try:
        raw = extract_from_webhook()
        if raw:
            normalized = transform_records(raw)
            count = load_records(normalized)
            mark_webhook_events_processed("webhook")
            _record_run(
                SourceType.WEBHOOK.value, "webhook", "success",
                extracted=len(raw), loaded=count, started_at=started,
            )
            total += count
            logger.info(f"[webhook] {count} records loaded")
    except Exception as exc:
        logger.error(f"[webhook] failed: {exc}")
        _record_run(
            SourceType.WEBHOOK.value, "webhook", "error",
            error=str(exc), started_at=started,
        )

    logger.info(f"Pipeline complete — total records loaded: {total}")
    return total


if __name__ == "__main__":
    main_flow()
