import json
import os
import re
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import quote

import requests

# Configuration
BASE_API_URL = os.getenv("KADASTER_BASE_API_URL", "https://data.labs.kadaster.nl/_api")
OUTPUT_DIR = os.getenv("KADASTER_OUTPUT_DIR", "kadaster_dataset")
USER_AGENT = os.getenv(
    "KADASTER_USER_AGENT",
    "KadasterDataExtractor/1.0 (Educational/Research Purpose)",
)
DELAY_BETWEEN_REQUESTS = float(os.getenv("KADASTER_DELAY_BETWEEN_REQUESTS", "0.1"))
REQUEST_TIMEOUT_SECONDS = float(os.getenv("KADASTER_TIMEOUT_SECONDS", "10"))
MAX_WORKERS = int(os.getenv("KADASTER_MAX_WORKERS", "10"))
SPARQL_ENDPOINT = os.getenv(
    "KADASTER_SPARQL_ENDPOINT",
    "https://data.labs.kadaster.nl/_api/datasets/kadaster/kkg/services/kkg/sparql",
)
SPARQL_REFERRER = os.getenv(
    "KADASTER_SPARQL_REFERRER",
    "https://data.labs.kadaster.nl/kadaster/kkg/sparql",
)
KADASTER_COOKIE = os.getenv("KADASTER_COOKIE")

# Headers to mimic a legitimate request
API_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
}

SPARQL_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "application/sparql-results+json, text/turtle",
    "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8,nl;q=0.7",
    "Content-Type": "application/json",
    "Origin": "https://data.labs.kadaster.nl",
    "Referer": SPARQL_REFERRER,
}
if KADASTER_COOKIE:
    SPARQL_HEADERS["Cookie"] = KADASTER_COOKIE


_thread_local = threading.local()


def get_thread_session():
    session = getattr(_thread_local, "session", None)
    if session is None:
        session = requests.Session()
        _thread_local.session = session
    return session


def setup_environment():
    """Creates the output directory if it doesn't exist."""
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        print(f"Created output directory: {OUTPUT_DIR}")


def fetch_catalog_page(session, page_num):
    """Fetches a single page of query results from the catalog."""
    url = f"{BASE_API_URL}/facets/queries?page={page_num}"
    try:
        response = session.get(url, headers=API_HEADERS, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"[!] Error fetching catalog page {page_num}: {e}")
        return None


def fetch_query_details(session, owner_account, query_name):
    """
    Fetches the full metadata and SPARQL code for a specific query.
    Corresponds to Request B in the documentation.
    """
    # URL encoding query name to handle spaces or special chars
    safe_name = quote(query_name)
    url = f"{BASE_API_URL}/queries/{owner_account}/{safe_name}"

    try:
        response = session.get(url, headers=API_HEADERS, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"[!] Error fetching details for {query_name}: {e}")
        return None


def extract_sparql_query(details):
    if not isinstance(details, dict):
        return None

    request_config = details.get("requestConfig")
    if isinstance(request_config, dict):
        payload = request_config.get("payload")
        if isinstance(payload, dict):
            for key in ("query", "sparql", "q"):
                value = payload.get(key)
                if isinstance(value, str) and value.strip():
                    return value
        elif isinstance(payload, str) and payload.strip():
            return payload

    for key in ("query", "sparql"):
        value = details.get(key)
        if isinstance(value, str) and value.strip():
            return value

    payload = details.get("payload")
    if isinstance(payload, dict):
        value = payload.get("query")
        if isinstance(value, str) and value.strip():
            return value

    return None


def clean_sparql_query(query):
    if not isinstance(query, str):
        return None

    cleaned = query.strip().lstrip("\ufeff")

    if len(cleaned) >= 2 and cleaned[0] == cleaned[-1] and cleaned[0] in ("'", '"'):
        inner = cleaned[1:-1].strip()
        if inner:
            cleaned = inner

    # Convert common escaped newlines/tabs found in scraped JSON-ish strings
    if "\\n" in cleaned:
        cleaned = cleaned.replace("\\r\\n", "\n").replace("\\n", "\n")
    if "\\t" in cleaned:
        cleaned = cleaned.replace("\\t", " ")

    # Normalize line endings and remove common invisible whitespace characters
    cleaned = cleaned.replace("\r\n", "\n").replace("\r", "\n")
    cleaned = cleaned.replace("\u00a0", " ").replace("\u200b", "").replace("\u200c", "").replace(
        "\u200d", ""
    )

    # Strip trailing whitespace per line, and drop common "caret marker" lines from error output.
    lines = []
    for line in cleaned.split("\n"):
        line = line.rstrip()
        if re.match(r"^-{3,}\^.*$", line):
            continue
        if re.match(r"^-{3,}\s*$", line):
            continue
        lines.append(line)

    # Collapse excessive blank lines
    normalized = "\n".join(lines)
    normalized = re.sub(r"\n{4,}", "\n\n\n", normalized).strip()
    return normalized or None


def execute_sparql(session, sparql_query, query_id):
    """
    Executes the SPARQL query against the target endpoint.
    Corresponds to Request C in the documentation.
    """
    if not sparql_query or not str(sparql_query).strip():
        return {"error": "No SPARQL query provided for execution"}

    exec_headers = SPARQL_HEADERS.copy()
    if query_id:
        exec_headers["x-t-queryid"] = str(query_id)

    payload = {"query": sparql_query}

    try:
        response = session.post(
            SPARQL_ENDPOINT,
            headers=exec_headers,
            json=payload,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        try:
            return response.json()
        except ValueError:
            text = response.text or ""
            return {
                "error": "Non-JSON response from SPARQL endpoint",
                "status_code": response.status_code,
                "content_type": response.headers.get("content-type"),
                "text_sample": text[:10000],
            }
    except requests.exceptions.RequestException as e:
        return {"error": f"Execution failed: {str(e)}"}

def save_entry(query_id, data):
    """Saves the combined data to a JSON file."""
    filename = os.path.join(OUTPUT_DIR, f"{query_id}.json")
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4, ensure_ascii=False)
    print(f"[+] Saved: {filename}")


def process_catalog_item(item):
    session = get_thread_session()

    q_id = item.get("id")
    q_name = item.get("name")
    q_owner = item.get("ownerAccountName")

    if not (q_name and q_owner):
        return 0

    print(f"    -> Processing: {q_name} (by {q_owner})")

    details = fetch_query_details(session, q_owner, q_name)
    if not details:
        return 0

    sparql_code = clean_sparql_query(extract_sparql_query(details))

    if sparql_code:
        print(f"       Executing SPARQL against {SPARQL_ENDPOINT}...")
        execution_results = execute_sparql(session, sparql_code, q_id)
    else:
        execution_results = {"info": "Skipped execution: No SPARQL code found in query details"}

    training_example = {
        "meta": {
            "id": q_id,
            "name": details.get("displayName"),
            "description": details.get("description"),
            "owner": details.get("owner", {}).get("name"),
            "visualization": details.get("renderConfig", {}).get("output"),
        },
        "prompt_context": {
            "prefixes": details.get("dataset", {}).get("prefixes", [])
            if isinstance(details.get("dataset"), dict)
            else [],
            "dataset_name": details.get("dataset", {}).get("displayName")
            if isinstance(details.get("dataset"), dict)
            else "Unknown",
        },
        "input_natural_language": details.get("description"),
        "output_sparql": sparql_code,
        "execution_result_sample": execution_results,
    }

    save_entry(q_id, training_example)

    if DELAY_BETWEEN_REQUESTS > 0:
        time.sleep(DELAY_BETWEEN_REQUESTS)

    return 1

def main():
    session = requests.Session()
    setup_environment()

    page = 1
    processed_count = 0

    print("--- Starting Extraction ---")

    while True:
        print(f"[*] Fetching catalog page {page}...")
        catalog_data = fetch_catalog_page(session, page)

        if not catalog_data:
            break

        results = catalog_data.get('results', [])
        if not results:
            print("[-] No more results found. Stopping.")
            break

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(process_catalog_item, item) for item in results]
            for future in as_completed(futures):
                try:
                    processed_count += future.result()
                except Exception as e:
                    print(f"[!] Error processing item: {e}")

        # Move to next page
        page += 1

    print(f"--- Finished. Total Queries Processed: {processed_count} ---")

if __name__ == "__main__":
    main()
