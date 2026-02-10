import os
import json
import csv
import datetime
import shutil
from typing import List, Optional


def _ensure_parent(path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)


def append_task_result(
    ticket_id: Optional[str],
    input_text: str,
    category: str,
    confidence: float,
    matched_keywords: List[str],
    execution_time_ms: int,
    output_json: str = "outputs/task_classifier_results.json",
    output_csv: Optional[str] = "outputs/task_classifier_results.csv",
):
    record = {
        "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
        "ticket_id": ticket_id,
        "input_text": input_text,
        "category": category,
        "confidence": confidence,
        "matched_keywords": matched_keywords,
        "execution_time_ms": execution_time_ms,
    }

    _ensure_parent(output_json)
    # Read existing array (if any), append the new record, and overwrite with a single JSON array
    try:
        if os.path.exists(output_json):
            with open(output_json, "r", encoding="utf-8") as fh:
                existing = fh.read().strip()
                if existing:
                    data = json.loads(existing)
                    if not isinstance(data, list):
                        # If file exists but is not an array, start fresh
                        data = []
                else:
                    data = []
        else:
            data = []
    except Exception:
        # If file is corrupted or unreadable, start new
        data = []

    data.append(record)

    # Prepare latest snapshot and archive folder
    output_dir = os.path.dirname(output_json) or "outputs"
    latest_file = os.path.join(output_dir, "task_classifier_results_latest.json")
    archive_dir = os.path.join(output_dir, "archive")
    os.makedirs(archive_dir, exist_ok=True)

    # Archive existing main snapshot if present (best-effort)
    try:
        if os.path.exists(output_json):
            ts = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
            archive_path = os.path.join(archive_dir, f"task_classifier_results_{ts}.json")
            try:
                shutil.copy2(output_json, archive_path)
            except Exception:
                # ignore archive failures
                pass
    except Exception:
        pass

    # Write atomically to avoid partial writes
    temp_path = output_json + ".tmp"
    with open(temp_path, "w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=2)
    os.replace(temp_path, output_json)

    # Also update latest snapshot atomically
    try:
        latest_temp = latest_file + ".tmp"
        with open(latest_temp, "w", encoding="utf-8") as fh:
            json.dump(data, fh, ensure_ascii=False, indent=2)
        os.replace(latest_temp, latest_file)
    except Exception:
        # Best-effort: do not fail main flow if latest snapshot fails
        pass

    # Optional: append to CSV for quick Excel consumption
    if output_csv:
        file_exists = os.path.exists(output_csv)
        _ensure_parent(output_csv)
        with open(output_csv, "a", newline="", encoding="utf-8") as csvfh:
            writer = csv.DictWriter(csvfh, fieldnames=[
                "timestamp", "ticket_id", "input_text", "category", "confidence", "matched_keywords", "execution_time_ms"
            ])
            if not file_exists:
                writer.writeheader()
            row = record.copy()
            row["matched_keywords"] = ";".join(row["matched_keywords"]) if row["matched_keywords"] else ""
            writer.writerow(row)


def append_audit_entry(
    audit_csv_path: str,
    ticket_number: str,
    ticket_id: str,
    old_category: str | None,
    new_category: str,
    confidence: float,
    run_timestamp: str,
    dry_run: bool = False,
):
    """Append an audit entry to the specified CSV file. Creates parent folders if needed."""
    _ensure_parent(audit_csv_path)
    file_exists = os.path.exists(audit_csv_path)

    import csv

    with open(audit_csv_path, "a", newline="", encoding="utf-8") as fh:
        fieldnames = [
            "run_timestamp",
            "ticket_number",
            "ticket_id",
            "old_category",
            "new_category",
            "confidence",
            "dry_run",
        ]
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerow({
            "run_timestamp": run_timestamp,
            "ticket_number": ticket_number,
            "ticket_id": ticket_id,
            "old_category": old_category or "",
            "new_category": new_category,
            "confidence": f"{confidence:.3f}",
            "dry_run": str(dry_run),
        })
