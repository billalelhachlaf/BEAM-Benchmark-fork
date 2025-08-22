wdc_file = "cleaned_output_wdc_triples_with_iata_filtered.txt"
wikidata_file = "attr_triples_wikidata_lower_normalized"
output_file = "ent_links"

# Step 1: Collect IATA codes and their subjects from both files
def extract_iata_wdc_subjects(file_path):
    iata_to_subjects = {}
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.lower()  # Convert line to lowercase
            if "<http://schema.org/iatacode>" in line:
                parts = line.strip().split("\t")
                if len(parts) >= 3:
                    subject = parts[0].strip()
                    code = parts[2].strip().strip('"')
                    if code not in iata_to_subjects:
                        iata_to_subjects[code] = set()
                    iata_to_subjects[code].add(subject)
    return iata_to_subjects

def extract_iata_wc_subjects(file_path):
    iata_to_subjects = {}
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            #line = line.lower()  # Convert line to lowercase
            if "http://www.wikidata.org/prop/p238" in line or "http://www.wikidata.org/prop/direct/p238" in line:
                parts = line.strip().split("\t")
                if len(parts) >= 3:
                    subject = parts[0].strip()
                    code = parts[2].strip().strip('"')
                    if code not in iata_to_subjects:
                        iata_to_subjects[code] = set()
                    iata_to_subjects[code].add(subject)
    return iata_to_subjects

# Extract subjects by IATA code from both files
wdc_iata_map = extract_iata_wdc_subjects(wdc_file)
wikidata_iata_map = extract_iata_wc_subjects(wikidata_file)

# Step 2: Match on common IATA codes and write pairs
with open(output_file, "w", encoding="utf-8") as out:
    for iata_code in wdc_iata_map:
        if iata_code in wikidata_iata_map:
            for wdc_subj in wdc_iata_map[iata_code]:
                for wd_subj in wikidata_iata_map[iata_code]:
                    out.write(f"{wdc_subj}\t{wd_subj}\n")

print(f"✅ Entity linking complete. Results saved to {output_file}")

