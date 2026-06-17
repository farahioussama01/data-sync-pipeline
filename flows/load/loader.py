from prefect import task, get_run_logger
from models.schemas import NormalizedRecord
from db.database import SessionLocal
from db.models import SyncedRecord
from sqlalchemy.dialects.postgresql import insert as pg_insert


@task(name="load-records")
def load_records(records: list[NormalizedRecord]) -> int:
    if not records:
        return 0

    logger = get_run_logger()
    logger.info(f"Loading {len(records)} records into PostgreSQL")
    db = SessionLocal()

    try:
        count = 0
        for rec in records:
            values = dict(
                source_type=rec.source_type.value,
                source_name=rec.source_name,
                external_id=rec.external_id,
                title=rec.title,
                body=rec.body,
                author=rec.author,
                category=rec.category,
                value=rec.value,
                tags=rec.tags or [],
                extra=rec.extra or {},
                source_created_at=rec.source_created_at,
                synced_at=rec.synced_at,
            )

            if rec.external_id:
                # Upsert: update on (source_name, external_id) conflict
                update_cols = {
                    k: v for k, v in values.items()
                    if k not in ("source_name", "external_id", "source_type")
                }
                stmt = (
                    pg_insert(SyncedRecord)
                    .values(**values)
                    .on_conflict_do_update(
                        index_elements=["source_name", "external_id"],
                        set_=update_cols,
                    )
                )
                db.execute(stmt)
            else:
                db.add(SyncedRecord(**values))

            count += 1

        db.commit()
        logger.info(f"Loaded {count} records")
        return count
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
