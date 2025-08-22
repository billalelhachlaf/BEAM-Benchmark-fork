import os
import time
import json
import re
from SPARQLWrapper import SPARQLWrapper, JSON, POST

WIKIDATA_ENDPOINT = "https://query.wikidata.org/sparql"
USER_AGENT = "EntityEnricher/1.0 (https://yourdomain.example)"
sparql = SPARQLWrapper(WIKIDATA_ENDPOINT)
sparql.setReturnFormat(JSON)
sparql.addCustomHttpHeader("User-Agent", USER_AGENT)
sparql.setMethod(POST)  # Ensure POST method is used

INPUT_FILES = ["attribute_wd_filtered.txt", "relational_wd_filtered.txt"]
OUTPUT_FILE = "wd_d1_entity_labels_descriptions.txt"
CACHE_FILE = "wd_d1_entity_labeldesc_cache.json"

BATCH_SIZE = 50
SLEEP_BETWEEN_REQUESTS = 2  # seconds


def extract_wikidata_uris(files):
    uris = set()
    for file in files:
        print(f"Reading from file: {file}")
        with open(file, "r", encoding="utf-8") as f:
            for line in f:
                parts = line.strip().split("\t")
                if len(parts) != 3:
                    continue
                prop, tail = parts[1], parts[2]
                if prop.startswith("http://www.wikidata.org"):
                    uris.add(prop)
                if tail.startswith("http://www.wikidata.org"):
                    uris.add(tail)
    return uris


def chunked(iterable, size):
    iterable = list(iterable)
    for i in range(0, len(iterable), size):
        yield iterable[i:i+size]


def fetch_labels_descriptions(uris):
    triples = []
    for idx, batch in enumerate(chunked(uris, BATCH_SIZE)):
        print(f"\n[Batch {idx + 1}] Processing batch of size {len(batch)}")
        values = " ".join(f"<{uri}>" for uri in batch)
        query = f"""
        SELECT ?s ?label ?desc WHERE {{
          VALUES ?s {{ {values} }}
          OPTIONAL {{ ?s rdfs:label ?label FILTER(LANG(?label) = "en") }}
          OPTIONAL {{ ?s schema:description ?desc FILTER(LANG(?desc) = "en") }}
        }}
        """
        retries = 3
        while retries > 0:
            try:
                local_sparql = SPARQLWrapper(WIKIDATA_ENDPOINT)
                local_sparql.setReturnFormat(JSON)
                local_sparql.addCustomHttpHeader("User-Agent", USER_AGENT)
                local_sparql.setMethod(POST)
                local_sparql.setQuery(query)

                results = local_sparql.query().convert()
                bindings = results["results"]["bindings"]
                print(f"Fetched {len(bindings)} results in this batch")

                for result in bindings:
                    s = result["s"]["value"]
                    if "label" in result:
                        label = result["label"]["value"]
                        triples.append((s, "http://www.w3.org/2000/01/rdf-schema#label", f'"{label}"'))
                    if "desc" in result:
                        desc = result["desc"]["value"]
                        triples.append((s, "http://schema.org/description", f'"{desc}"'))
                break  # success
            except Exception as e:
                print(f"Error fetching batch, retrying... ({e})")
                retries -= 1
                time.sleep(SLEEP_BETWEEN_REQUESTS * 2)
        time.sleep(SLEEP_BETWEEN_REQUESTS)
    return triples


def load_cache():
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_cache(cache):
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2, ensure_ascii=False)


def main():
    print("🔍 Extracting Wikidata URIs from input files...")
    all_uris = extract_wikidata_uris(INPUT_FILES)
    print(f"📌 Total unique URIs found: {len(all_uris)}")

    cache = load_cache()
    print(f"🗃️  Cached URIs: {len(cache)}")

    uris_to_query = [uri for uri in all_uris if uri not in cache]
    print(f"🚀 URIs to query (excluding cached): {len(uris_to_query)}")

    if not uris_to_query:
        print("✅ Nothing to query. All URIs are already cached.")
    else:
        new_triples = fetch_labels_descriptions(uris_to_query)
        print(f"✅ Total new triples fetched: {len(new_triples)}")

        # Update cache
        for s, p, o in new_triples:
            if s not in cache:
                cache[s] = {}
            if p.endswith("label"):
                cache[s]["label"] = o
            elif p.endswith("description"):
                cache[s]["description"] = o

        print(f"💾 Saving updated cache (size: {len(cache)})")
        save_cache(cache)

    print(f"📝 Writing triples to {OUTPUT_FILE}...")
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        for uri, info in cache.items():
            if "label" in info:
                f.write(f"{uri}\thttp://www.w3.org/2000/01/rdf-schema#label\t{info['label']}\n")
            if "description" in info:
                f.write(f"{uri}\thttp://schema.org/description\t{info['description']}\n")

    print("🎉 Done!")


if __name__ == "__main__":
    main()

