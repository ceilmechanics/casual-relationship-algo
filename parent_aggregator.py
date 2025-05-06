

import pandas as pd
import os

# Input CSV file path
input_csv = "sibling-for-analysis/sibling_MS_30541_MS_3533.csv"  # Replace with actual input path

# Output directory
output_dir = "aggregator/"
os.makedirs(output_dir, exist_ok=True)

# Read the CSV file
df = pd.read_csv(input_csv)

# Group by 'um'
for um_value, group in df.groupby("um"):
    # Extract the necessary columns
    subset = group[["um", "execution_order", "dm1", "dm1_start_time", "dm2", "dm2_start_time"]]

    # Determine filename
    if not subset.empty:
        dm1 = subset.iloc[0]["dm1"]
        dm2 = subset.iloc[0]["dm2"]
        filename = f"{um_value}.csv"
        output_path = os.path.join(output_dir, filename)

        # Save to CSV
        subset.to_csv(output_path, index=False)
        print(f"Saved: {output_path}")