import requests
import json
from collections import defaultdict, OrderedDict
import os
import time

LABEL_CACHE_FILE = "label_cache_wiki_props.json"
OUTPUT_FILE = "sorted_wiki_props.json"

USER_AGENT = "YourAppName/1.0 (your.email@example.com) Python script to fetch Wikidata labels"


def get_property_label(prop_url, cache, max_retries=3):
    if prop_url in cache:
        return cache[prop_url]

    if "wikidata.org/prop/direct/" in prop_url or "wikidata.org/prop/" in prop_url:
        prop_id = prop_url.rstrip('/').split('/')[-1]

        sparql_url = "https://query.wikidata.org/sparql"
        query = f"""
        SELECT ?label WHERE {{
          wd:{prop_id} rdfs:label ?label .
          FILTER (lang(?label) = "en")
        }}
        """
        headers = {"Accept": "application/sparql-results+json", "User-Agent": USER_AGENT}
        timeout = 20  # Increased timeout to 20 seconds
        retries = 0
        label = "Unknown"

        while retries < max_retries:
            try:
                response = requests.get(sparql_url, params={"query": query}, headers=headers, timeout=timeout)
                response.raise_for_status()
                data = response.json()
                results = data.get("results", {}).get("bindings", [])
                if results:
                    label = results[0]["label"]["value"]
                break  # Success, exit retry loop
            except requests.exceptions.ReadTimeout:
                retries += 1
                wait_time = 2 ** retries  # exponential backoff: 2, 4, 8 sec
                print(f"Read timeout for {prop_id}, retry {retries}/{max_retries} after {wait_time}s")
                time.sleep(wait_time)
            except requests.exceptions.HTTPError as e:
                print(f"Failed to fetch label for {prop_id}: {e}")
                break
            except requests.exceptions.RequestException as e:
                print(f"Request error for {prop_id}: {e}")
                break

        time.sleep(1)  # Rate limit delay

    else:
        label = prop_url

    cache[prop_url] = label
    return label


def load_label_cache():
    if os.path.exists(LABEL_CACHE_FILE):
        with open(LABEL_CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_label_cache(cache):
    with open(LABEL_CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2, ensure_ascii=False)


def process_triples(file_path):
    prop_count = defaultdict(int)

    print("Counting properties in file...")
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split("\t")
            if len(parts) == 3:
                prop = parts[1]
                prop_count[prop] += 1

    label_cache = load_label_cache()
    final_dict = {}

    print("Fetching labels (from cache or Wikidata)...")
    for prop, count in prop_count.items():
        label = get_property_label(prop, label_cache)
        final_dict[prop] = [count, label]

    save_label_cache(label_cache)

    print("Sorting properties by frequency...")
    sorted_dict = OrderedDict(sorted(final_dict.items(), key=lambda x: x[1][0]))

    with open(OUTPUT_FILE, "w", encoding="utf-8") as out_f:
        json.dump(sorted_dict, out_f, indent=2, ensure_ascii=False)

    print(f"Done! Results saved to '{OUTPUT_FILE}'")
    return sorted_dict

if __name__ == "__main__":
    # Replace 'triples.txt' with your actual file path if different
    process_triples("triples.txt")

