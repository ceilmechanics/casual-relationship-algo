#!/usr/bin/env python3
import os
import pandas as pd
import glob
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor
import time

def find_ms_metrics(msname, timestamp, metrics_path):
    """Find microservice metrics (CPU and memory) in MSMetrics folder using closest timestamp."""
    cpu = None
    memory = None
    min_time_diff = float('inf')
    
    csv_files = glob.glob(os.path.join(metrics_path, '*.csv'))
    print(f"Searching for {msname} metrics at timestamp {timestamp}")
    
    for csv_file in csv_files:
        try:
            # Try to read only necessary columns first to check if file contains matching data
            df = pd.read_csv(csv_file, usecols=['msname'])
            
            # Skip file if no matching service records
            if not any(df['msname'] == msname):
                continue
            
            # Read full file if matches found
            df = pd.read_csv(csv_file, usecols=['timestamp', 'msname', 'cpu_utilization', 'memory_utilization'])
            
            # Filter by msname
            matches = df[df['msname'] == msname]
            
            if not matches.empty:
                # Sort by timestamp
                matches = matches.sort_values('timestamp')

                # Find closest timestamp
                for _, row in matches.iterrows():
                    time_diff = abs(row['timestamp'] - timestamp)
                    if time_diff < min_time_diff:
                        min_time_diff = time_diff
                        cpu = row.get('cpu_utilization')
                        memory = row.get('memory_utilization') if 'memory_utilization' in df.columns else None
                
        except Exception as e:
            print(f"Error processing {os.path.basename(csv_file)}: {str(e)}")
    
    if cpu is not None:
        print(f"Found metrics for {msname}: CPU={cpu}, Memory={memory}, time lag={min_time_diff}ms")
        return cpu, memory, min_time_diff
    else:
        print(f"No metrics found for {msname}")
        return None, None, None

def find_mcr(msname, timestamp, msrtmcr_path):
    """Find MCR in MSRTMCR folder using closest timestamp and calculate average if multiple values exist."""
    mcr_values = []
    min_time_diff = float('inf')
    mcr_column = 'providerrpc_mcr'
    
    csv_files = glob.glob(os.path.join(msrtmcr_path, '*.csv'))
    print(f"Searching for {msname} MCR at timestamp {timestamp}")
    
    for csv_file in csv_files:
        try:
            # Try to read only necessary columns first
            df = pd.read_csv(csv_file, usecols=['msname'])
            
            # Skip file if no matching service records
            if not any(df['msname'] == msname):
                continue
            
            # Read full file if matches found
            df = pd.read_csv(csv_file, usecols=['timestamp', 'msname', mcr_column])
            
            # Filter by msname
            matches = df[df['msname'] == msname]
            
            if not matches.empty:
                # Sort by timestamp
                matches = matches.sort_values('timestamp')
                
                # Find the rows with timestamp closest to the target timestamp
                current_min_diff = min_time_diff
                
                for _, row in matches.iterrows():
                    current_ts = row['timestamp']
                    time_diff = abs(current_ts - timestamp)
                    
                    # If this is a new minimum, reset our collection
                    if time_diff < current_min_diff:
                        current_min_diff = time_diff
                        mcr_values = [row.get(mcr_column)]
                    # If this is equal to current minimum, add to collection
                    elif time_diff == current_min_diff:
                        mcr_values.append(row.get(mcr_column))
                
                # Update overall minimum if we found better matches
                if current_min_diff < min_time_diff:
                    min_time_diff = current_min_diff
                
        except Exception as e:
            print(f"Error processing {os.path.basename(csv_file)}: {str(e)}")
    
    # Calculate average MCR if values were found
    if mcr_values:
        # Filter out None values
        valid_mcr_values = [v for v in mcr_values if v is not None]
        if valid_mcr_values:
            # Print individual values for debugging
            print(f"Individual MCR values: {valid_mcr_values}")
            
            avg_mcr = sum(valid_mcr_values) / len(valid_mcr_values)
            print(f"Found {len(valid_mcr_values)} MCR values for {msname}, average: {avg_mcr}")
            return avg_mcr, min_time_diff
    
    print(f"No MCR found for {msname}")
    return None, None

def process_row(args):
    """Process a single row (for parallel processing)."""
    idx, row, total_rows, ms_metrics_path, msrtmcr_path = args
    
    try:
        # Extract values from row
        traceid = row.get('traceid')
        um = row.get('um')
        dm1 = row.get('dm1')
        dm1_start_time = row.get('dm1_start_time')
        dm2 = row.get('dm2')
        dm2_start_time = row.get('dm2_start_time')
        execution_order = row.get('execution_order')

        # Find metrics for both microservices
        dm1_cpu, dm1_memory, dm1_system_lag = find_ms_metrics(dm1, dm1_start_time, ms_metrics_path)
        dm1_mcr, dm1_mcr_lag = find_mcr(dm1, dm1_start_time, msrtmcr_path)
        dm2_cpu, dm2_memory, dm2_system_lag = find_ms_metrics(dm2, dm2_start_time, ms_metrics_path)
        dm2_mcr, dm2_mcr_lag = find_mcr(dm2, dm2_start_time, msrtmcr_path)

        # Create output row
        output_row = {
            'traceid': traceid,
            'um': um,
            'dm1': dm1,
            'dm2': dm2,
            'execution_order': execution_order,
            'dm1_cpu': dm1_cpu,
            'dm2_cpu': dm2_cpu,
            'dm1_memory': dm1_memory,
            'dm2_memory': dm2_memory,
            'dm1_system_lag': dm1_system_lag,
            'dm2_system_lag': dm2_system_lag,
            'dm1_mcr': dm1_mcr,
            'dm2_mcr': dm2_mcr,
            'dm1_mcr_lag': dm1_mcr_lag,
            'dm2_mcr_lag': dm2_mcr_lag
        }

        print(f"Processed row {idx+1}/{total_rows}")
        return output_row

    except Exception as e:
        print(f"Error processing row {idx+1}: {str(e)}")
        return None

def process_input_csv(input_csv_path, chunk_size=1000):
    """Process the input CSV and collect metrics.
    
    Args:
        input_csv_path: Path to input CSV file
        chunk_size: Number of rows to process before writing to CSV (0 = write all at once)
    """
    print(f"\nProcessing: {input_csv_path}")
    start_time = time.time()
    
    # Paths for metrics files
    base_path = 'output/data'
    ms_metrics_path = os.path.join(base_path, 'MSMetrics')
    msrtmcr_path = os.path.join(base_path, 'MSRTMCR')
    
    # Read input CSV
    try:
        input_df = pd.read_csv(input_csv_path)
        print(f"Read input CSV with {len(input_df)} rows")
    except Exception as e:
        print(f"Error reading input CSV: {str(e)}")
        return
    
    # Generate output filename
    dm_pairs = input_df[['dm1', 'dm2']].drop_duplicates()
    if len(dm_pairs) > 0:
        first_pair = dm_pairs.iloc[0]
        output_csv_name = f"contextual_{first_pair['dm1']}_{first_pair['dm2']}.csv"
    else:
        output_csv_name = "contextual_unknown_unknown.csv"
    
    output_csv_path = os.path.join('output', output_csv_name)
    os.makedirs(os.path.dirname(output_csv_path), exist_ok=True)
    
    # Process rows
    output_data = []
    use_parallel = len(input_df) > 10  # Use parallel processing for larger datasets
    
    # Function to write chunks to CSV
    def write_chunk(data_chunk, is_first_chunk):
        if not data_chunk:
            return
        
        mode = 'w' if is_first_chunk else 'a'
        header = is_first_chunk
        
        chunk_df = pd.DataFrame(data_chunk)
        chunk_df.to_csv(output_csv_path, mode=mode, header=header, index=False)
        print(f"Wrote {len(chunk_df)} rows to {output_csv_path}")
    
    # Process sequentially with chunking
    if not use_parallel or chunk_size > 0:
        is_first_chunk = True
        current_chunk = []
        
        for idx, row in input_df.iterrows():
            args = (idx, row, len(input_df), ms_metrics_path, msrtmcr_path)
            result = process_row(args)
            
            if result:
                current_chunk.append(result)
                
                # Write chunk if we've reached chunk_size
                if chunk_size > 0 and len(current_chunk) >= chunk_size:
                    write_chunk(current_chunk, is_first_chunk)
                    is_first_chunk = False
                    current_chunk = []
        
        # Write any remaining rows
        if current_chunk:
            write_chunk(current_chunk, is_first_chunk)
    
    # Process in parallel and write all at once
    else:
        print(f"Processing {len(input_df)} rows in parallel...")
        args_list = [(idx, row, len(input_df), ms_metrics_path, msrtmcr_path) 
                     for idx, row in input_df.iterrows()]
        
        with ProcessPoolExecutor(max_workers=min(mp.cpu_count(), len(input_df))) as executor:
            results = list(executor.map(process_row, args_list))
            
        output_data = [result for result in results if result]
        
        # Write all results at once
        if output_data:
            write_chunk(output_data, True)
    
    elapsed_time = time.time() - start_time
    print(f"Completed in {elapsed_time:.2f} seconds.")

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Gather contextual metrics for microservices.')
    parser.add_argument('input_csv', help='Path to input CSV file')
    parser.add_argument('--chunk-size', type=int, default=0, 
                        help='Number of rows to write per chunk (0 = write all at once)')
    
    args = parser.parse_args()
    
    print("\nCONTEXTUAL METRICS GATHERING TOOL (SIMPLIFIED VERSION)")
    print(f"Input CSV: {args.input_csv}")
    print(f"Output Directory: output/")
    
    process_input_csv(args.input_csv, args.chunk_size)

if __name__ == "__main__":
    main()