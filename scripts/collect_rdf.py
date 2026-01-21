from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def is_rdf_execution_result(execution_result: Any) -> bool:
    if not isinstance(execution_result, dict):
        return False

    if execution_result.get("error"):
        return False

    result_format = execution_result.get("result_format")
    if result_format == "turtle":
        return True

    content_type = (execution_result.get("content_type") or "").lower()
    if "turtle" in content_type:
        return True

    # Backward compatibility: earlier versions stored Turtle in an "error" object.
    # If it's not marked as error and includes a Turtle content-type, treat as RDF.
    text_sample = execution_result.get("text_sample")
    return isinstance(text_sample, str) and len(text_sample) > 0 and "turtle" in content_type


def compact_rdf_execution_result(execution_result: dict[str, Any], max_chars: int) -> dict[str, Any]:
    compact: dict[str, Any] = {}
    for key in ("result_format", "status_code", "content_type"):
        if key in execution_result:
            compact[key] = execution_result.get(key)

    text = execution_result.get("text_sample")
    if not isinstance(text, str):
        text = ""

    text = text.strip()
    compact["text_sample_truncated"] = len(text) > max_chars
    compact["text_sample"] = text[:max_chars]
    compact["text_sample_chars"] = len(compact["text_sample"])
    compact["text_sample_lines"] = compact["text_sample"].count("\n") + (1 if compact["text_sample"] else 0)
    # Heuristic: Turtle often ends statements with " ."
    compact["statement_terminators_estimate"] = compact["text_sample"].count(" .")
    return compact


def main() -> int:
    source_dir = Path(os.getenv("KADASTER_OUTPUT_DIR", "kadaster_dataset"))
    max_chars = int(os.getenv("KADASTER_RDF_TEXT_LIMIT", "20000"))

    if not source_dir.exists():
        print(f"[!] Source dir not found: {source_dir}")
        return 2

    items: dict[str, Any] = {}
    scanned = 0
    rdf_found = 0

    for path in sorted(source_dir.glob("*.json")):
        scanned += 1
        try:
            obj = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue

        exec_result = obj.get("execution_result_sample")
        if not is_rdf_execution_result(exec_result):
            continue

        rdf_found += 1

        meta = obj.get("meta") if isinstance(obj.get("meta"), dict) else {}
        key = str(meta.get("id") or path.stem)
        if isinstance(exec_result, dict):
            obj["execution_result_sample"] = compact_rdf_execution_result(exec_result, max_chars)
        obj["execution_result_compacted"] = True
        items[key] = obj

    out = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_dir": str(source_dir),
        "scanned_files": scanned,
        "rdf_found": rdf_found,
        "rdf_text_limit_applied": max_chars,
        "included_count": len(items),
        "items": items,
    }

    out_path = Path("kadaster_rdf_results.json")
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(
        f"[+] Wrote {out_path} with {len(items)} RDF results "
        f"(text sample limit {max_chars}, scanned {scanned})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

