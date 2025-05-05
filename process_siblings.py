#!/usr/bin/env python3
"""
Process sibling CSV files from output/siblings directory
Categorize them into parallel, unknown, and uncertain based on execution_order pattern
"""

import pandas as pd
import os
import shutil
import glob
from pathlib import Path

def process_sibling_files():
    """Process sibling CSV files and categorize them"""
    
    # Create output directories
    os.makedirs("output/res", exist_ok=True)
    os.makedirs("output/res/uncertain", exist_ok=True)
    
    # Initialize result dataframes
    parallel_data = []
    unknown_data = []
    
    # Track statistics for uncertain files
    largest_uncertain_service = None
    max_uncertain_observations = 0
    
    # Get all CSV files from siblings directory
    sibling_files = glob.glob("output/siblings/*.csv")
    
    print(f"Found {len(sibling_files)} CSV files in output/siblings directory")
    print("-" * 50)
    
    for csv_file in sibling_files:
        filename = os.path.basename(csv_file)
        
        # Skip files with (?) or 'unknown' in the name
        if "(?)" in filename or "unknown" in filename.lower():
            print(f"Skipping: {filename} (contains '(?)' or 'unknown')")
            continue
        
        try:
            # Read CSV file
            df = pd.read_csv(csv_file)
            
            # Extract information from filename
            # Assuming format: traceid_um_dm1_dm2.csv
            name_parts = filename.replace('.csv', '').split('_')
            if len(name_parts) >= 4:
                traceid = name_parts[0]
                um = name_parts[1]
                dm1 = name_parts[2]
                dm2 = name_parts[3]
            else:
                print(f"Warning: Cannot parse filename {filename}")
                continue
            
            # Get number of observations - directly using len(df)
            num_observations = len(df)
            
            # Check if execution_order column exists
            if 'execution_order' not in df.columns:
                print(f"Warning: No execution_order column in {filename}")
                continue
            
            # Check execution_order values
            execution_orders = df['execution_order']
            
            # Check if all are concurrent (not parallel)
            all_concurrent = all(order == 'concurrent' for order in execution_orders)
            
            # Count sequential and concurrent
            num_concurrent = sum(1 for order in execution_orders if order == 'concurrent')
            num_seq = sum(1 for order in execution_orders if order == 'sequential')
            
            if all_concurrent:
                # All concurrent - add to parallel.csv
                parallel_data.append({
                    'traceid': traceid,
                    'um': um,
                    'dm1': dm1,
                    'dm2': dm2,
                    'num_observations': num_observations
                })
                print(f"Parallel: {filename} ({num_observations} observations)")
            else:
                # Mixed execution orders
                if num_observations < 1000:
                    # Small dataset - add to unknown.csv
                    unknown_data.append({
                        'traceid': traceid,
                        'um': um,
                        'dm1': dm1,
                        'dm2': dm2,
                        'num_seq': num_seq,
                        'num_parallel': num_concurrent,  # Changed to num_concurrent
                        'num_observations': num_observations
                    })
                    print(f"Unknown: {filename} ({num_observations} observations)")
                else:
                    # Large dataset - copy to uncertain folder
                    output_path = os.path.join("output/res/uncertain", filename)
                    shutil.copy2(csv_file, output_path)
                    print(f"Uncertain: {filename} ({num_observations} observations)")
                    
                    # Track largest uncertain service
                    if num_observations > max_uncertain_observations:
                        max_uncertain_observations = num_observations
                        largest_uncertain_service = filename
        
        except Exception as e:
            print(f"Error processing {filename}: {e}")
    
    # Save parallel.csv
    if parallel_data:
        parallel_df = pd.DataFrame(parallel_data)
        parallel_df.to_csv("output/res/parallel.csv", index=False)
        print(f"\nCreated output/res/parallel.csv with {len(parallel_data)} entries")
    
    # Save unknown.csv
    if unknown_data:
        unknown_df = pd.DataFrame(unknown_data)
        unknown_df.to_csv("output/res/unknown.csv", index=False)
        print(f"Created output/res/unknown.csv with {len(unknown_data)} entries")
    
    # Print largest uncertain service
    print("\n" + "="*50)
    if largest_uncertain_service:
        print(f"Largest uncertain service: {largest_uncertain_service}")
        print(f"Number of observations: {max_uncertain_observations}")
    else:
        print("No uncertain files found")
    print("="*50)

if __name__ == "__main__":
    process_sibling_files()