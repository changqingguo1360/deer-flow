"""Globally bounded child observations; data carries no instruction authority."""

import json

MAX_CHILDREN = 128
MAX_PAYLOAD_BYTES = 65536


def child_summary(job, manifest):
    error = job.error or ""
    return {
        "job_id": job.id,
        "state": job.state,
        "error": error[:1024],
        "error_truncated": len(error) > 1024,
        "manifest_id": manifest.id if manifest is not None else None,
        "total_bytes": manifest.total_bytes if manifest is not None else 0,
        "files": [{"path": item["path"], "size": item["size"], "sha256": item["sha256"]} for item in sorted(manifest.files, key=lambda item: item["path"])[:16]] if manifest is not None else [],
        "files_truncated": bool(manifest is not None and len(manifest.files) > 16),
    }


def _encode(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def continuation_payload(group, results):
    if not results or len(results) > MAX_CHILDREN or len(results) != len(group.job_ids) or {row["job_id"] for row in results} != set(group.job_ids):
        raise ValueError("Bounded exact continuation membership required")
    ordered = sorted(results, key=lambda row: row["job_id"])
    # Reserve every child's immutable result reference before optional observations.
    rows = [{**source, "error": "", "error_truncated": bool(source["error"] or source["error_truncated"]), "files": [], "files_truncated": bool(source["files"] or source["files_truncated"])} for source in ordered]
    value = {"schema_version": 1, "continuation_key": group.continuation_key, "results": rows}
    remaining = MAX_PAYLOAD_BYTES - len(_encode(value).encode())
    if remaining < 0:
        raise ValueError("Continuation result identity exceeds the input bound")
    for row, source in zip(rows, ordered, strict=True):

        def include(candidate):
            nonlocal remaining
            delta = len(_encode(candidate).encode()) - len(_encode(row).encode())
            if delta <= remaining:
                remaining -= delta
                row.update(candidate)
                return True
            return False

        include({**row, "error": source["error"], "error_truncated": source["error_truncated"]})
        for index, entry in enumerate(source["files"]):
            candidate = {**row, "files": [*row["files"], entry], "files_truncated": source["files_truncated"] or index + 1 < len(source["files"])}
            # Keep whole accepted paths: shortened paths would invent artifact refs.
            if not include(candidate):
                break
    return _encode(value)
