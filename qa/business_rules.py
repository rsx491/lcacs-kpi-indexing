from collections import Counter
from dataclasses import dataclass, field
import re


RELEASE_PREFIX = "/lca-collaboration/ws/release/"

REQUEST_RE = re.compile(
    r'"(GET|POST|PUT|DELETE|PATCH|HEAD|OPTIONS)\s+(\S+)\s+HTTP/[^"]+"\s+(\d{3})'
)


@dataclass
class ReleaseActivityValidation:
    release_endpoint_lines: int = 0
    malformed_release_requests: int = 0
    counts: Counter = field(default_factory=Counter)


def validate_release_activity(source_path: str) -> ReleaseActivityValidation:
    result = ReleaseActivityValidation()

    with open(source_path, "rt", errors="replace") as source:
        for line in source:
            # Cheap first filter so regex runs only on relevant lines.
            if RELEASE_PREFIX not in line:
                continue

            match = REQUEST_RE.search(line)

            if not match:
                result.malformed_release_requests += 1
                continue

            method, request_path, status_text = match.groups()

            if RELEASE_PREFIX not in request_path:
                continue

            result.release_endpoint_lines += 1
            status = int(status_text)

            if status != 200:
                result.counts["release_endpoint_non_200"] += 1
            elif method == "POST":
                result.counts["release_creation"] += 1
            elif method == "PUT":
                result.counts["release_update"] += 1
            elif method == "GET":
                result.counts["release_info_view"] += 1
            else:
                result.counts["release_endpoint_other_200"] += 1

    return result