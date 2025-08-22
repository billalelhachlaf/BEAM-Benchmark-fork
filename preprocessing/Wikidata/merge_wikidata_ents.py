from collections import defaultdict

# Input files
wikidata_file = "attr_triples_wd_lower"
rel_triples_file = "rel_triples_2_lower"
output_attr_file = "attr_triples_wd_lower_normalized"
output_rel_file = "rel_triples_2_lower_normalized"

# Step 1: Extract IATA code → subject ID mapping
iata_to_subjects = defaultdict(set)

with open(wikidata_file, "r", encoding="utf-8") as f:
    for line in f:
        if "http://www.wikidata.org/prop/p238" in line or "http://www.wikidata.org/prop/direct/p238" in line:
            parts = line.strip().split("\t")
            if len(parts) >= 3:
                subject = parts[0].strip()
                iata_code = parts[2].strip().strip('"')
                iata_to_subjects[iata_code].add(subject)

# Step 2: Build replacement mapping
replacement_map = {}
for subjects in iata_to_subjects.values():
    if len(subjects) > 1:
        sorted_subjects = sorted(subjects)
        canonical_subject = sorted_subjects[0]
        for s in sorted_subjects[1:]:
            replacement_map[s] = canonical_subject

print("🔁 Replacement mapping:")
for k, v in replacement_map.items():
    print(f"{k} → {v}")

# Helper function to replace subject IDs
def replace_subjects_in_line(line, mapping):
    for old, new in mapping.items():
        if old in line:
            line = line.replace(old, new)
    return line

# Step 3: Replace in attr_triples_wd_lower
with open(wikidata_file, "r", encoding="utf-8") as fin, \
     open(output_attr_file, "w", encoding="utf-8") as fout:
    for line in fin:
        new_line = replace_subjects_in_line(line, replacement_map)
        fout.write(new_line)

# Step 4: Replace in rel_triples_2_lower
with open(rel_triples_file, "r", encoding="utf-8") as fin, \
     open(output_rel_file, "w", encoding="utf-8") as fout:
    for line in fin:
        new_line = replace_subjects_in_line(line, replacement_map)
        fout.write(new_line)

print("\n✅ Normalization complete. Output saved to:")
print(f"- {output_attr_file}")
print(f"- {output_rel_file}")

