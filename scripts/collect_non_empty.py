from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def is_non_empty_execution_result(execution_result: Any) -> bool:
    if not isinstance(execution_result, dict):
        return False

    if execution_result.get("error"):
        return False

    results = execution_result.get("results")
    if not isinstance(results, dict):
        return False

    bindings = results.get("bindings")
    return isinstance(bindings, list) and len(bindings) > 0


def compact_execution_result(execution_result: dict[str, Any], max_bindings: int) -> dict[str, Any]:
    results = execution_result.get("results")
    head = execution_result.get("head")

    compact: dict[str, Any] = {}
    if isinstance(head, dict):
        compact["head"] = head

    if isinstance(results, dict):
        bindings = results.get("bindings")
        if isinstance(bindings, list):
            compact["results"] = {
                "bindings_count": len(bindings),
                "bindings_sample_count": min(len(bindings), max_bindings),
                "bindings_truncated": len(bindings) > max_bindings,
                "bindings": bindings[:max_bindings],
            }
        else:
            compact["results"] = results

    # Preserve boolean ASK responses if present
    if "boolean" in execution_result:
        compact["boolean"] = execution_result.get("boolean")

    # Preserve lightweight format hints if present
    for key in ("result_format", "status_code", "content_type", "text_sample"):
        if key in execution_result:
            compact[key] = execution_result.get(key)

    return compact


def main() -> int:
    source_dir = Path(os.getenv("KADASTER_OUTPUT_DIR", os.path.join("data", "kadaster_dataset")))
    max_bindings = int(os.getenv("KADASTER_BINDINGS_LIMIT", "25"))
    if not source_dir.exists():
        print(f"[!] Source dir not found: {source_dir}")
        return 2

    items: dict[str, Any] = {}
    scanned = 0
    non_empty_found = 0

    for path in sorted(source_dir.glob("*.json")):
        scanned += 1
        try:
            obj = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue

        exec_result = obj.get("execution_result_sample")
        if not is_non_empty_execution_result(exec_result):
            continue

        non_empty_found += 1

        meta = obj.get("meta") if isinstance(obj.get("meta"), dict) else {}
        key = str(meta.get("id") or path.stem)
        if isinstance(exec_result, dict):
            obj["execution_result_sample"] = compact_execution_result(exec_result, max_bindings)
        obj["execution_result_compacted"] = True
        items[key] = obj

    out = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_dir": str(source_dir),
        "scanned_files": scanned,
        "non_empty_found": non_empty_found,
        "bindings_limit_applied": max_bindings,
        "included_count": len(items),
        "items": items,
    }

    out_path = Path(os.getenv("KADASTER_NON_EMPTY_OUT", os.path.join("data", "kadaster_non_empty_results.json")))
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(
        f"[+] Wrote {out_path} with {len(items)} non-empty results "
        f"(bindings sample limit {max_bindings}, scanned {scanned})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
