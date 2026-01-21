from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any
from urllib.parse import quote

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    def load_dotenv(*_args: Any, **_kwargs: Any) -> None:  # type: ignore[no-redef]
        return None

try:
    from fastmcp import FastMCP
except ImportError as e:  # pragma: no cover
    raise SystemExit(
        "Missing dependency: fastmcp. Install with `uv sync` (or `pip install fastmcp httpx`)."
    ) from e

try:
    import httpx
except ImportError as e:  # pragma: no cover
    raise SystemExit(
        "Missing dependency: httpx. Install with `uv sync` (or `pip install fastmcp httpx python-dotenv`)."
    ) from e

load_dotenv()

mcp = FastMCP("Kadaster Knowledge Graph Expert")

DEFAULT_EXAMPLES_PATH = Path(os.getenv("MCP_EXAMPLES_PATH", "data/fewshot_topk.json"))

KADASTER_SPARQL_ENDPOINT = os.getenv(
    "KADASTER_SPARQL_ENDPOINT",
    "https://data.labs.kadaster.nl/_api/datasets/kadaster/kkg/services/kkg/sparql",
)
KADASTER_SPARQL_REFERRER = os.getenv(
    "KADASTER_SPARQL_REFERRER",
    "https://data.labs.kadaster.nl/kadaster/kkg/sparql",
)
KADASTER_COOKIE = os.getenv("KADASTER_COOKIE")
REQUEST_TIMEOUT_SECONDS = float(os.getenv("KADASTER_TIMEOUT_SECONDS", "10"))

MAX_FEWSHOT_EXAMPLES = 3
MAX_BINDINGS_RETURNED = int(os.getenv("MCP_MAX_BINDINGS_RETURNED", "25"))
DEFAULT_QUERY_LIMIT = int(os.getenv("MCP_DEFAULT_QUERY_LIMIT", "100"))
MAX_ROWS_PREVIEW = int(os.getenv("MCP_MAX_ROWS_PREVIEW", "5"))
MAX_VALUE_CHARS = int(os.getenv("MCP_MAX_VALUE_CHARS", "200"))

PREFIXES = """\
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX imxgeo: <http://modellen.geostandaarden.nl/def/imx-geo#>
PREFIX ext: <https://modellen.kkg.kadaster.nl/def/imxgeo-ext#>
PREFIX prov: <http://www.w3.org/ns/prov#>
PREFIX bag: <https://bag.basisregistraties.overheid.nl/def/bag#>
PREFIX nen3610: <http://modellen.geostandaarden.nl/def/nen3610#>
"""


def _http_headers(accept: str) -> dict[str, str]:
    headers = {
        "Accept": accept,
        "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8,nl;q=0.7",
        "Content-Type": "application/json",
        "Origin": "https://data.labs.kadaster.nl",
        "Referer": KADASTER_SPARQL_REFERRER,
        "User-Agent": os.getenv(
            "KADASTER_USER_AGENT",
            "KadasterDataExtractor/1.0 (Educational/Research Purpose)",
        ),
    }
    if KADASTER_COOKIE:
        headers["Cookie"] = KADASTER_COOKIE
    return headers


_SPARQL_UPDATE_RE = re.compile(r"\b(insert|delete|load|clear|create|drop|copy|move|add)\b", re.I)
_LIMIT_RE = re.compile(r"\blimit\s+\d+\b", re.I)


def _is_select_query(query: str) -> bool:
    return bool(re.search(r"\bselect\b", query, flags=re.I))


def _looks_like_update(query: str) -> bool:
    return bool(_SPARQL_UPDATE_RE.search(query))


def _ensure_limit(query: str, limit: int) -> str:
    if _LIMIT_RE.search(query):
        return query
    if not _is_select_query(query):
        return query
    return query.rstrip() + f"\nLIMIT {limit}\n"


def _safe_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _synth_description(name: str | None, tags: list[str] | None) -> str:
    n = (name or "").strip() or "Unnamed query"
    t = ", ".join((tags or [])[:8]).strip()
    if t:
        return f"Query '{n}' using patterns: {t}"
    return f"Query '{n}'"


_TOKEN_RE = re.compile(r"[a-z0-9][a-z0-9_-]{1,}", re.I)


def _tokens(text: str) -> set[str]:
    return {t.lower() for t in _TOKEN_RE.findall(text or "")}


def get_relevant_examples(user_intent: str, top_k: int) -> list[dict[str, Any]]:
    """
    Select the most relevant examples for a user intent using lightweight lexical scoring.

    This keeps prompt context small while steering the LLM toward the right ontology/patterns.
    """
    intent = (user_intent or "").strip().lower()
    intent_tokens = _tokens(intent)
    if not intent_tokens:
        return EXAMPLES_CACHE[:top_k]

    scored: list[tuple[int, dict[str, Any]]] = []
    for ex in EXAMPLES_CACHE:
        score = 0

        tags = ex.get("tags") or []
        if isinstance(tags, list):
            for tag in tags:
                tag_s = str(tag).lower()
                if not tag_s:
                    continue
                if tag_s in intent:
                    score += 6
                score += 2 * len(intent_tokens.intersection(_tokens(tag_s)))

        desc = str(ex.get("natural_language") or "")
        score += len(intent_tokens.intersection(_tokens(desc)))

        name = str(ex.get("name") or "")
        score += 2 * len(intent_tokens.intersection(_tokens(name)))

        # Tie-break: prefer already-curated items when scores are similar
        if ex.get("final_score") is not None:
            score += 1

        scored.append((score, ex))

    scored.sort(key=lambda x: x[0], reverse=True)

    def example_key(ex: dict[str, Any]) -> str:
        ex_id = ex.get("id")
        if isinstance(ex_id, str) and ex_id.strip():
            return f"id:{ex_id.strip()}"
        sparql = ex.get("sparql")
        if isinstance(sparql, str) and sparql.strip():
            return f"sparql:{hash(sparql.strip())}"
        name = ex.get("name")
        if isinstance(name, str) and name.strip():
            return f"name:{name.strip().lower()}"
        return f"obj:{id(ex)}"

    chosen: list[dict[str, Any]] = []
    seen_keys: set[str] = set()

    # Take best-scoring unique examples first
    for _score, ex in scored:
        if not isinstance(ex, dict):
            continue
        key = example_key(ex)
        if key in seen_keys:
            continue
        chosen.append(ex)
        seen_keys.add(key)
        if len(chosen) >= top_k:
            break

    # Always return exactly top_k if possible (pad from cache if needed)
    if len(chosen) < top_k:
        for ex in EXAMPLES_CACHE:
            if not isinstance(ex, dict):
                continue
            key = example_key(ex)
            if key in seen_keys:
                continue
            chosen.append(ex)
            seen_keys.add(key)
            if len(chosen) >= top_k:
                break

    return chosen[:top_k]


def load_fewshot_examples(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []

    items = data.get("items")
    if not isinstance(items, list):
        # Expected shape from our notebook export: {"top_k":..., "items":[...]}
        items = []

    processed: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue

        name = item.get("name")
        sparql = item.get("sparql")
        if not isinstance(sparql, str) or not sparql.strip():
            continue

        judge = item.get("judge") if isinstance(item.get("judge"), dict) else {}
        tags = judge.get("tags") if isinstance(judge.get("tags"), list) else []
        tags = [str(t) for t in tags if isinstance(t, (str, int, float))]

        nl = item.get("natural_language")
        if not isinstance(nl, str) or not nl.strip():
            nl = _synth_description(name if isinstance(name, str) else None, tags)

        processed.append(
            {
                "id": str(item.get("id") or ""),
                "name": name if isinstance(name, str) else None,
                "natural_language": nl,
                "sparql": sparql.strip(),
                "tags": tags,
                "final_score": item.get("final_score"),
                "kind": item.get("kind"),
            }
        )

    return processed


EXAMPLES_CACHE = load_fewshot_examples(DEFAULT_EXAMPLES_PATH)


@mcp.tool()
async def execute_kadaster_sparql(
    query: str,
    accept: str = "application/sparql-results+json",
    max_bindings: int | None = None,
    default_limit: int | None = None,
) -> str:
    """
    Execute a SPARQL query against the Kadaster KKG endpoint.

    - Blocks SPARQL update operations.
    - Optionally appends a LIMIT for SELECT queries if none is present.
    - Returns a compact response to keep context small.
    """
    if not isinstance(query, str) or not query.strip():
        return json.dumps({"status": "error", "error": "Missing query"}, indent=2)

    q = query.strip()
    if _looks_like_update(q):
        return json.dumps({"status": "error", "error": "Only read-only queries are allowed"}, indent=2)

    limit = _safe_int(default_limit, DEFAULT_QUERY_LIMIT)
    q = _ensure_limit(q, limit)

    mb = _safe_int(max_bindings, MAX_BINDINGS_RETURNED)
    mb = max(1, min(mb, MAX_BINDINGS_RETURNED))
    max_rows = max(1, min(MAX_ROWS_PREVIEW, mb))

    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT_SECONDS) as client:
        try:
            resp = await client.post(
                KADASTER_SPARQL_ENDPOINT,
                headers=_http_headers(accept=accept),
                json={"query": q},
            )
            resp.raise_for_status()
        except httpx.HTTPStatusError as e:
            return json.dumps(
                {
                    "status": "http_error",
                    "status_code": e.response.status_code,
                    "content_type": e.response.headers.get("content-type"),
                    "text_sample": (e.response.text or "")[:5000],
                },
                indent=2,
                ensure_ascii=False,
            )
        except Exception as e:
            return json.dumps({"status": "error", "error": str(e)}, indent=2, ensure_ascii=False)

        content_type = (resp.headers.get("content-type") or "").lower()
        if "json" in content_type:
            data = resp.json()
            bindings = data.get("results", {}).get("bindings", [])
            if not isinstance(bindings, list):
                bindings = []

            def compact_row(row: Any) -> dict[str, Any]:
                if not isinstance(row, dict):
                    return {}
                out: dict[str, Any] = {}
                for var, value_obj in row.items():
                    if not isinstance(value_obj, dict):
                        continue
                    val = str(value_obj.get("value", ""))
                    if len(val) > MAX_VALUE_CHARS:
                        val = val[:MAX_VALUE_CHARS] + "...[TRUNCATED]"
                    out[str(var)] = {
                        "type": value_obj.get("type"),
                        "value": val,
                        "datatype": value_obj.get("datatype"),
                        "lang": value_obj.get("xml:lang") or value_obj.get("lang"),
                    }
                return out

            preview_rows = [compact_row(r) for r in bindings[:max_rows]]
            return json.dumps(
                {
                    "status": "success",
                    "content_type": resp.headers.get("content-type"),
                    "result_count": len(bindings),
                    "displayed_results": len(preview_rows),
                    "note": (
                        "Results are truncated to prevent context overload. "
                        "Refine the query (filters/variables) for more specific output."
                    ),
                    "results_preview": preview_rows,
                    "vars": data.get("head", {}).get("vars", []),
                },
                indent=2,
                ensure_ascii=False,
            )

        # Turtle / text results (e.g. CONSTRUCT/DESCRIBE)
        text = resp.text or ""
        return json.dumps(
            {
                "status": "success",
                "content_type": resp.headers.get("content-type"),
                "text_sample": text[:20000],
                "text_sample_truncated": len(text) > 20000,
            },
            indent=2,
            ensure_ascii=False,
        )


@mcp.tool()
async def list_fewshot_examples(tag: str | None = None, limit: int = 10) -> str:
    """
    List curated few-shot examples. Optionally filter by tag substring.
    """
    examples = EXAMPLES_CACHE
    if tag and isinstance(tag, str) and tag.strip():
        needle = tag.strip().lower()
        examples = [
            ex
            for ex in examples
            if any(needle in str(t).lower() for t in (ex.get("tags") or []))
            or (ex.get("name") and needle in str(ex.get("name")).lower())
        ]

    lim = _safe_int(limit, 10)
    lim = max(1, min(lim, 50))
    out = []
    for ex in examples[:lim]:
        out.append(
            {
                "id": ex.get("id"),
                "name": ex.get("name"),
                "natural_language": ex.get("natural_language"),
                "tags": ex.get("tags"),
                "kind": ex.get("kind"),
                "final_score": ex.get("final_score"),
            }
        )
    return json.dumps({"count": len(out), "examples": out}, indent=2, ensure_ascii=False)


@mcp.tool()
async def kadaster_sparql_ui_link(query: str) -> str:
    """
    Generate a Kadaster Labs UI link for a SPARQL query (best-effort; URL schema may change).
    """
    base = "https://data.labs.kadaster.nl/kadaster/kkg/sparql"
    encoded = quote(query or "")
    return json.dumps(
        {
            "ui_base": base,
            "query_param_link": f"{base}?query={encoded}",
            "fragment_link": f"{base}#query={encoded}",
        },
        indent=2,
    )


@mcp.prompt()
def kkg_query_builder(user_intent: str) -> str:
    """
    Few-shot prompt that teaches the KKG ontology & patterns using curated examples.
    """
    intent = (user_intent or "").strip() or "Query geospatial data"
    examples = get_relevant_examples(intent, top_k=MAX_FEWSHOT_EXAMPLES)

    examples_context = ""
    for i, ex in enumerate(examples, start=1):
        examples_context += (
            f"\n### Example {i}\n"
            f"Goal: {ex.get('natural_language')}\n"
            f"Tags: {', '.join(ex.get('tags') or [])}\n"
            f"SPARQL:\n{ex.get('sparql')}\n"
            f"---\n"
        )

    return f"""You are an expert SPARQL engineer for the Dutch Kadaster Knowledge Graph (KKG).

Write a SPARQL query for this user request:
"{intent}"

Rules:
1) Always include these standard prefixes:
{PREFIXES}
2) Prefer `SELECT` queries unless the user explicitly needs an RDF graph output.
3) Always use `LIMIT` (max {DEFAULT_QUERY_LIMIT}) to prevent timeouts.
4) If geometry is needed, include a WKT literal using `geo:asWKT` (often via `ext:bovenaanzichtgeometrie`).
5) Link to BAG provenance using `prov:wasDerivedFrom` when applicable.

Few-shot examples (study patterns and ontology usage):
{examples_context}

Return ONLY the SPARQL query text.
"""


if __name__ == "__main__":
    mcp.run()
