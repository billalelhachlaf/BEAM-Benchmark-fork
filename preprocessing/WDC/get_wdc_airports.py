input_file = "triples_1.txt"  #initial_wdc_triples.txt
output_file = "wdc_airport_related_triples.txt"

airport_type = "<http://schema.org/Airport>"
airport_entities = set()
all_triples = []

# First pass: read and collect all triples, find airport entities
with open(input_file, "r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        all_triples.append(line)
        if line.endswith(airport_type):
            entity = line.split("\t")[0]
            airport_entities.add(entity)

# Second pass: filter triples that start with airport entities
relevant_triples = [triple for triple in all_triples if triple.split("\t")[0] in airport_entities]

# Write the output
with open(output_file, "w", encoding="utf-8") as f:
    for triple in relevant_triples:
        f.write(triple + "\n")

# Print results
print(f"Number of airport entities found: {len(airport_entities)}")
print(f"Saved {len(relevant_triples)} relevant triples to {output_file}")

