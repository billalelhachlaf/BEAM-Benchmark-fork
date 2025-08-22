#!/bin/bash

# List of input datasets
datasets=("airport" "books")
# Repeat for 5 folds
for fold in {1..5}; do
  for dataset in "${datasets[@]}"; do

    input_path="data/${dataset}/ent_links"
    output_dir="data/${dataset}/271_5fold/${fold}/"

    python3 <<EOF
import os
import random

random.seed($fold)  # Different seed for each fold

# Paths
input_path = "$input_path"
output_dir = "$output_dir"
os.makedirs(output_dir, exist_ok=True)

# Read data
with open(input_path, "r") as f:
    lines = f.readlines()

# Shuffle
random.shuffle(lines)

# Compute sizes
total = len(lines)
train_size = int(0.7 * total)
test_size = int(0.2 * total)
valid_size = total - train_size - test_size

# Split
train_lines = lines[:train_size]
test_lines = lines[train_size:train_size + test_size]
valid_lines = lines[train_size + test_size:]

# Save splits
with open(os.path.join(output_dir, "train_links"), "w") as f:
    f.writelines(train_lines)

with open(os.path.join(output_dir, "test_links"), "w") as f:
    f.writelines(test_lines)

with open(os.path.join(output_dir, "valid_links"), "w") as f:
    f.writelines(valid_lines)
EOF

  done
done

