import math
from typing import Any, Optional
from datetime import datetime

from prefect import task, get_run_logger
from models.schemas import RawRecord, NormalizedRecord, SourceType


def _parse_dt(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        f = float(value)
        return None if math.isnan(f) else f
    except (TypeError, ValueError):
        return None


def _normalize_api(raw: dict) -> dict:
    known = {
        "id", "title", "name", "body", "description",
        "userId", "user_id", "category", "value", "price",
        "tags", "createdAt", "created_at",
    }
    return {
        "external_id": str(raw["id"]) if "id" in raw else None,
        "title": raw.get("title") or raw.get("name"),
        "body": raw.get("body") or raw.get("description"),
        "author": (
            f"user_{raw['userId']}" if "userId" in raw
            else f"user_{raw['user_id']}" if "user_id" in raw
            else None
        ),
        "category": raw.get("category"),
        "value": _safe_float(raw.get("value") or raw.get("price")),
        "tags": raw["tags"] if isinstance(raw.get("tags"), list) else [],
        "extra": {k: v for k, v in raw.items() if k not in known},
        "source_created_at": _parse_dt(raw.get("createdAt") or raw.get("created_at")),
    }


def _normalize_csv(raw: dict) -> dict:
    field_map = {
        "id": "external_id",
        "title": "title", "name": "title",
        "body": "body", "description": "body", "content": "body",
        "author": "author",
        "category": "category",
        "value": "value", "price": "value", "amount": "value",
    }
    result: dict[str, Any] = {}
    known: set[str] = set()

    for src, dst in field_map.items():
        if src in raw and raw[src] is not None and dst not in result:
            known.add(src)
            if dst == "external_id":
                result["external_id"] = str(raw[src])
            elif dst == "value":
                result["value"] = _safe_float(raw[src])
            else:
                result[dst] = str(raw[src])

    result.setdefault("tags", [])
    result["extra"] = {k: v for k, v in raw.items() if k not in known}
    result["source_created_at"] = _parse_dt(raw.get("created_at") or raw.get("date"))
    return result


def _normalize_webhook(raw: dict) -> dict:
    known = {
        "id", "title", "event", "type", "body", "message", "description",
        "author", "user", "sender", "category", "event_type",
        "value", "amount", "tags", "timestamp", "created_at",
    }
    return {
        "external_id": str(raw["id"]) if "id" in raw else None,
        "title": raw.get("title") or raw.get("event") or raw.get("type"),
        "body": raw.get("body") or raw.get("message") or raw.get("description"),
        "author": raw.get("author") or raw.get("user") or raw.get("sender"),
        "category": raw.get("category") or raw.get("event_type"),
        "value": _safe_float(raw.get("value") or raw.get("amount")),
        "tags": raw["tags"] if isinstance(raw.get("tags"), list) else [],
        "extra": {k: v for k, v in raw.items() if k not in known},
        "source_created_at": _parse_dt(raw.get("timestamp") or raw.get("created_at")),
    }


_NORMALIZERS = {
    SourceType.API: _normalize_api,
    SourceType.CSV: _normalize_csv,
    SourceType.WEBHOOK: _normalize_webhook,
}


@task(name="transform-records")
def transform_records(raw_records: list[RawRecord]) -> list[NormalizedRecord]:
    logger = get_run_logger()
    logger.info(f"Transforming {len(raw_records)} records")

    normalized, errors = [], 0
    for raw in raw_records:
        try:
            fields = _NORMALIZERS[raw.source_type](raw.raw_data)
            normalized.append(
                NormalizedRecord(
                    source_type=raw.source_type,
                    source_name=raw.source_name,
                    synced_at=datetime.utcnow(),
                    **fields,
                )
            )
        except Exception as e:
            errors += 1
            logger.warning(f"Transform error ({raw.source_name}): {e}")

    logger.info(f"Transformed {len(normalized)} records ({errors} errors)")
    return normalized
