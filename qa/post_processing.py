from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from elasticsearch import Elasticsearch


@dataclass
class PostProcessingResult:
    index_name: str

    index_exists: bool = False
    document_count: int = 0

    missing_required_fields: dict[str, int] = field(default_factory=dict)

    earliest_timestamp: Optional[str] = None
    latest_timestamp: Optional[str] = None

    before_start: int = 0
    at_or_after_end: int = 0

    passed: bool = False
    errors: list[str] = field(default_factory=list)


def validate_index(
    es: Elasticsearch,
    index_name: str,
    start_date: datetime,
    end_date: datetime,
    timestamp_field: str = "@timestamp",
    required_fields: tuple[str, ...] = (),
) -> PostProcessingResult:

    result = PostProcessingResult(index_name=index_name)

    # 1. Index must exist.
    if not es.indices.exists(index=index_name):
        result.errors.append(f"Index does not exist: {index_name}")
        return result

    result.index_exists = True

    # 2. Count indexed documents.
    result.document_count = es.count(index=index_name)["count"]

    # 3. Find the indexed timestamp range.
    response = es.search(
        index=index_name,
        size=0,
        aggs={
            "earliest": {"min": {"field": timestamp_field}},
            "latest": {"max": {"field": timestamp_field}},
        },
    )

    result.earliest_timestamp = (
        response["aggregations"]["earliest"].get("value_as_string")
    )
    result.latest_timestamp = (
        response["aggregations"]["latest"].get("value_as_string")
    )

    # 4. Detect records outside the reporting period.
    result.before_start = es.count(
        index=index_name,
        query={
            "range": {
                timestamp_field: {
                    "lt": start_date.isoformat()
                }
            }
        },
    )["count"]

    result.at_or_after_end = es.count(
        index=index_name,
        query={
            "range": {
                timestamp_field: {
                    "gte": end_date.isoformat()
                }
            }
        },
    )["count"]

    if result.before_start:
        result.errors.append(
            f"{result.before_start} documents occur before reporting start"
        )

    if result.at_or_after_end:
        result.errors.append(
            f"{result.at_or_after_end} documents occur at/after reporting end"
        )

    # 5. Required fields must be populated.
    for field_name in required_fields:
        missing = es.count(
            index=index_name,
            query={
                "bool": {
                    "must_not": {
                        "exists": {"field": field_name}
                    }
                }
            },
        )["count"]

        result.missing_required_fields[field_name] = missing

        if missing:
            result.errors.append(
                f"{missing} documents missing required field: {field_name}"
            )

    result.passed = len(result.errors) == 0

    return result