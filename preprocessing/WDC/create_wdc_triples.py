import re

# Regex for extracting triples (subject, predicate, object)
pattern = re.compile(r'(<[^>]+>|_:[^\s]+)\s+(<[^>]+>)\s+(".*?"(?:\^\^<[^>]+>)?|<[^>]+>|_:[^\s]+)\s+<[^>]+>\s+\.')
input_file = "parts"  # Path to your large input file from wdc online
output_file = "triples_1.txt"  # Path to the output file

# Function to process the file in chunks
def process_large_file(input_file, output_file, chunk_size=100000):
    with open(input_file, "r", encoding="utf-8") as infile, open(output_file, "w", encoding="utf-8") as outfile:
        triplets = []
        for line_count, line in enumerate(infile, start=1):
            match = pattern.match(line.strip())
            if match:
                triplets.append("\t".join(match.groups()))  # Store in tab-separated format
            # Write to the file in chunks to avoid memory overload
            if line_count % chunk_size == 0:
                outfile.write("\n".join(triplets) + "\n")
                triplets.clear()  # Clear the list to free memory

        # Write any remaining triplets (if any) after the loop ends
        if triplets:
            outfile.write("\n".join(triplets) + "\n")
    
    print(f"✅ Extraction completed! {line_count} lines processed. Triplets saved to `{output_file}`.")

# Run the function
process_large_file(input_file, output_file, chunk_size=100000)

