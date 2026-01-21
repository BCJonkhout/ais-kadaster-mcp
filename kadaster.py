import requests
import json
import time
import os
from urllib.parse import quote

# Configuration
BASE_API_URL = "https://data.labs.kadaster.nl/_api"
OUTPUT_DIR = "kadaster_dataset"
USER_AGENT = "KadasterDataExtractor/1.0 (Educational/Research Purpose)"
DELAY_BETWEEN_REQUESTS = 0.01  # Seconds (Be polite to the server)
GLOBAL_EXECUTION_ENDPOINT = "https://api.labs.kadaster.nl/datasets/kadaster/kkg/sparql" # New: User specified global endpoint for execution

# Headers to mimic a legitimate request
HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
}

def setup_environment():
    """Creates the output directory if it doesn't exist."""
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        print(f"Created output directory: {OUTPUT_DIR}")

def fetch_catalog_page(page_num):
    """Fetches a single page of query results from the catalog."""
    url = f"{BASE_API_URL}/facets/queries?page={page_num}"
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"[!] Error fetching catalog page {page_num}: {e}")
        return None

def fetch_query_details(owner_account, query_name):
    """
    Fetches the full metadata and SPARQL code for a specific query.
    Corresponds to Request B in the documentation.
    """
    # URL encoding query name to handle spaces or special chars
    safe_name = quote(query_name)
    url = f"{BASE_API_URL}/queries/{owner_account}/{safe_name}"

    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"[!] Error fetching details for {query_name}: {e}")
        return None

def execute_sparql(service_url, sparql_query, query_id):
    """
    Executes the SPARQL query against the target endpoint.
    Corresponds to Request C in the documentation.
    """
    if not service_url:
        return {"error": "No service URL provided for execution"}

    # specific headers for execution
    exec_headers = HEADERS.copy()
    exec_headers["Content-Type"] = "application/json"
    exec_headers["x-t-queryid"] = query_id # Link execution to query ID

    payload = {
        "query": sparql_query
    }

    try:
        response = requests.post(service_url, headers=exec_headers, json=payload, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        return {"error": f"Execution failed: {str(e)}"}

def save_entry(query_id, data):
    """Saves the combined data to a JSON file."""
    filename = os.path.join(OUTPUT_DIR, f"{query_id}.json")
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4, ensure_ascii=False)
    print(f"[+] Saved: {filename}")

def main():
    setup_environment()

    page = 1
    processed_count = 0

    print("--- Starting Extraction ---")

    while True:
        print(f"[*] Fetching catalog page {page}...")
        catalog_data = fetch_catalog_page(page)

        if not catalog_data:
            break

        results = catalog_data.get('results', [])
        if not results:
            print("[-] No more results found. Stopping.")
            break

        for item in results:
            # 1. Extract basic info from Catalog
            q_id = item.get('id')
            q_name = item.get('name')
            q_owner = item.get('ownerAccountName')

            if not (q_name and q_owner):
                continue

            print(f"    -> Processing: {q_name} (by {q_owner})")

            # 2. Get Full Details (SPARQL Code)
            details = fetch_query_details(q_owner, q_name)

            if details:
                # Extract crucial parts
                sparql_code = details.get('requestConfig', {}).get('payload', {}).get('query')

                # Use the global execution endpoint for all queries
                service_url = GLOBAL_EXECUTION_ENDPOINT

                # 3. Execute Query (Optional - can be disabled to save time/bandwidth)
                execution_results = None
                if sparql_code and service_url:
                    # Only execute if we have a valid endpoint
                    # Be careful: some queries return massive datasets.
                    print(f"       Executing SPARQL against {service_url}...")
                    execution_results = execute_sparql(service_url, sparql_code, q_id)
                else:
                    execution_results = {"info": "Skipped execution: No SPARQL code or service URL defined"}

                # 4. Compile the training example
                training_example = {
                    "meta": {
                        "id": q_id,
                        "name": details.get('displayName'),
                        "description": details.get('description'),
                        "owner": details.get('owner', {}).get('name'),
                        "visualization": details.get('renderConfig', {}).get('output')
                    },
                    "prompt_context": {
                        "prefixes": details.get('dataset', {}).get('prefixes', []) if isinstance(details.get('dataset'), dict) else [],
                        "dataset_name": details.get('dataset', {}).get('displayName') if isinstance(details.get('dataset'), dict) else "Unknown"
                    },
                    "input_natural_language": details.get('description'),
                    "output_sparql": sparql_code,
                    "execution_result_sample": execution_results
                }

                save_entry(q_id, training_example)
                processed_count += 1

            # Respect rate limits
            time.sleep(DELAY_BETWEEN_REQUESTS)

        # Move to next page
        page += 1

    print(f"--- Finished. Total Queries Processed: {processed_count} ---")

if __name__ == "__main__":
    main()