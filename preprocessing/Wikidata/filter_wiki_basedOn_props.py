import json

# Step 1: Load the sorted_wiki_props.json
with open('sorted_wiki_props.json', 'r', encoding='utf-8') as f:
    sorted_props = json.load(f)

# Step 2: Build filter set
excluded_labels = {
    "http://schema.org/version",
    "http://schema.org/dateModified",
    "http://wikiba.se/ontology#sitelinks",
    "image",
    "OpenStreetMap node ID",
    "http://wikiba.se/ontology#identifiers",
    "http://wikiba.se/ontology#statements",
    "different from"
}

exceptions = {"short name", "official name", "location"}

# Build set of properties to remove
props_to_remove = set()

for prop, (count, label) in sorted_props.items():
    if label in excluded_labels:
        props_to_remove.add(prop)
    elif count <= 3487 and label not in exceptions:
        props_to_remove.add(prop)

print(f"Total properties to remove: {len(props_to_remove)}")

# Step 3: Define a function to filter files
def filter_file(input_path, output_path, props_to_remove):
    with open(input_path, 'r', encoding='utf-8') as infile, \
         open(output_path, 'w', encoding='utf-8') as outfile:
        for line in infile:
            parts = line.strip().split('\t')
            if len(parts) >= 2:
                prop = parts[1]
                if prop not in props_to_remove:
                    outfile.write(line)

# Step 4: Filter both files
filter_file('attribute_wd.txt', 'attribute_wd_filtered.txt', props_to_remove)
filter_file('relational_wd.txt', 'relational_wd_filtered.txt', props_to_remove)

print("Filtering complete. Output written to:")
print(" - attribute_wd_filtered.txt")
print(" - relational_wd_filtered.txt")

