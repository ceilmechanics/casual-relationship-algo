#!/usr/bin/env python3
import os
import pandas as pd
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor
import time
import pickle

# Time interval in milliseconds (60 seconds * 1000)
TIME_INTERVAL = 60 * 1000
# Maximum number of intervals to try (5 before + 5 after = 10 minutes total)
MAX_INTERVALS = 5

def load_metrics_index():
    """Load metrics index from the predefined location."""
    index_path = 'output/data/MSMetrics/index.pkl'
    try:
        print(f"Loading metrics index from: {index_path}")
        with open(index_path, 'rb') as f:
            metrics_index = pickle.load(f)
        print(f"Loaded metrics index with {len(metrics_index)} time intervals")
        return metrics_index
    except Exception as e:
        print(f"Error loading metrics index: {str(e)}")
        raise

def load_mcr_index():
    """Load MCR index from the predefined location."""
    index_path = 'output/data/MSRTMCR/index.pkl'
    try:
        print(f"Loading MCR index from: {index_path}")
        with open(index_path, 'rb') as f:
            mcr_index = pickle.load(f)
        print(f"Loaded MCR index with {len(mcr_index)} time intervals")
        return mcr_index
    except Exception as e:
        print(f"Error loading MCR index: {str(e)}")
        raise

def find_ms_metrics_optimized(msname, timestamp, metrics_index):
    """Find microservice metrics using expanding radius search."""
    # Align timestamp to interval
    base_interval = (timestamp // TIME_INTERVAL) * TIME_INTERVAL
    
    # First check the exact interval
    if base_interval in metrics_index and msname in metrics_index[base_interval]:
        records = metrics_index[base_interval][msname]
        if records:
            # Take the first record since timestamps are already aligned
            record = records[0]
            cpu = record.get('cpu_utilization')
            memory = record.get('memory_utilization')
            # Calculate actual time difference
            time_diff = abs(record['timestamp'] - timestamp)
            return cpu, memory, time_diff
    
    # If not found in exact interval, search outward with increasing radius
    for radius in range(1, MAX_INTERVALS + 1):
        # Check interval to the left
        left_interval = base_interval - (radius * TIME_INTERVAL)
        if left_interval in metrics_index and msname in metrics_index[left_interval]:
            records = metrics_index[left_interval][msname]
            if records:
                record = records[0]
                cpu = record.get('cpu_utilization')
                memory = record.get('memory_utilization')
                # Calculate actual time difference
                time_diff = abs(record['timestamp'] - timestamp)
                return cpu, memory, time_diff
        
        # Check interval to the right
        right_interval = base_interval + (radius * TIME_INTERVAL)
        if right_interval in metrics_index and msname in metrics_index[right_interval]:
            records = metrics_index[right_interval][msname]
            if records:
                record = records[0]
                cpu = record.get('cpu_utilization')
                memory = record.get('memory_utilization')
                # Calculate actual time difference
                time_diff = abs(record['timestamp'] - timestamp)
                return cpu, memory, time_diff
    
    # If we get here, no match was found within the radius
    return None, None, None

def find_mcr_optimized(msname, timestamp, mcr_index):
    """Find MCR using expanding radius search."""
    # Align timestamp to interval
    base_interval = (timestamp // TIME_INTERVAL) * TIME_INTERVAL
    
    # First check the exact interval
    if base_interval in mcr_index and msname in mcr_index[base_interval]:
        records = mcr_index[base_interval][msname]
        if records:
            # With aligned timestamps, we may still have multiple MCR values
            # so we average them as in the original code
            mcr_values = [record.get('mcr') for record in records if record.get('mcr') is not None]
            if mcr_values:
                avg_mcr = sum(mcr_values) / len(mcr_values)
                # Calculate actual time difference - using the timestamp of the first record
                time_diff = abs(records[0]['timestamp'] - timestamp)
                return avg_mcr, time_diff
    
    # If not found in exact interval, search outward with increasing radius
    for radius in range(1, MAX_INTERVALS + 1):
        # Check interval to the left
        left_interval = base_interval - (radius * TIME_INTERVAL)
        if left_interval in mcr_index and msname in mcr_index[left_interval]:
            records = mcr_index[left_interval][msname]
            if records:
                mcr_values = [record.get('mcr') for record in records if record.get('mcr') is not None]
                if mcr_values:
                    avg_mcr = sum(mcr_values) / len(mcr_values)
                    # Calculate actual time difference - using the timestamp of the first record
                    time_diff = abs(records[0]['timestamp'] - timestamp)
                    return avg_mcr, time_diff
        
        # Check interval to the right
        right_interval = base_interval + (radius * TIME_INTERVAL)
        if right_interval in mcr_index and msname in mcr_index[right_interval]:
            records = mcr_index[right_interval][msname]
            if records:
                mcr_values = [record.get('mcr') for record in records if record.get('mcr') is not None]
                if mcr_values:
                    avg_mcr = sum(mcr_values) / len(mcr_values)
                    # Calculate actual time difference - using the timestamp of the first record
                    time_diff = abs(records[0]['timestamp'] - timestamp)
                    return avg_mcr, time_diff
    
    # If we get here, no match was found within the radius
    return None, None

def process_row_optimized(args):
    """Process a single row with optimized lookup."""
    idx, row, metrics_index, mcr_index = args
    
    try:
        # Extract values from row
        um = row.get('um')
        dm1 = row.get('dm1')
        dm1_start_time = row.get('dm1_start_time')
        dm2 = row.get('dm2')
        dm2_start_time = row.get('dm2_start_time')
        execution_order = row.get('execution_order')

        # Find metrics for both microservices using optimized functions
        dm1_cpu, dm1_memory, dm1_system_lag = find_ms_metrics_optimized(dm1, dm1_start_time, metrics_index)
        dm1_mcr, dm1_mcr_lag = find_mcr_optimized(dm1, dm1_start_time, mcr_index)
        dm2_cpu, dm2_memory, dm2_system_lag = find_ms_metrics_optimized(dm2, dm2_start_time, metrics_index)
        dm2_mcr, dm2_mcr_lag = find_mcr_optimized(dm2, dm2_start_time, mcr_index)

        # Create output row
        output_row = {
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

        return output_row

    except Exception as e:
        print(f"Error processing row {idx+1}: {str(e)}")
        return None

def process_input_csv_optimized(input_csv_path, chunk_size=1000, use_parallel=True, max_workers=None):
    """Process the input CSV with optimized metrics lookup using existing indexes."""
    print(f"\nProcessing: {input_csv_path}")
    start_time = time.time()
    
    # Load pre-built indexes from the predefined locations
    metrics_index = load_metrics_index()
    mcr_index = load_mcr_index()
    
    # Read input CSV
    try:
        input_df = pd.read_csv(input_csv_path)
        total_rows = len(input_df)
        print(f"Read input CSV with {total_rows} rows")
    except Exception as e:
        print(f"Error reading input CSV: {str(e)}")
        return
    
    # Generate output filename
    unique_ums = input_df['um'].unique()
    if len(unique_ums) > 0:
        # Use the first um value for the filename
        first_um = unique_ums[0]
        output_csv_name = f"contextual_{first_um}.csv"
    else:
        output_csv_name = "contextual_unknown_um.csv"
    
    output_csv_path = os.path.join('output', output_csv_name)
    os.makedirs(os.path.dirname(output_csv_path), exist_ok=True)
    
    # Function to write chunks to CSV
    def write_chunk(data_chunk, is_first_chunk):
        if not data_chunk:
            return
        
        mode = 'w' if is_first_chunk else 'a'
        header = is_first_chunk
        
        chunk_df = pd.DataFrame(data_chunk)
        chunk_df.to_csv(output_csv_path, mode=mode, header=header, index=False)
        print(f"Wrote {len(chunk_df)} rows to {output_csv_path}")
    
    # Determine if we should use parallel processing
    use_parallel = use_parallel and total_rows > 10
    
    if not max_workers:
        max_workers = min(mp.cpu_count(), total_rows)
    
    # Process in sequential mode
    if not use_parallel:
        print(f"Processing {total_rows} rows sequentially...")
        is_first_chunk = True
        current_chunk = []
        
        for idx, row in input_df.iterrows():
            if idx % 10 == 0:  # Print progress every 10 rows
                print(f"Processing row {idx+1}/{total_rows} ({idx/total_rows*100:.1f}%)")
            
            args = (idx, row, metrics_index, mcr_index)
            result = process_row_optimized(args)
            
            if result:
                current_chunk.append(result)
                
                # Write chunk if we've reached chunk_size
                if len(current_chunk) >= chunk_size:
                    write_chunk(current_chunk, is_first_chunk)
                    is_first_chunk = False
                    current_chunk = []
        
        # Write any remaining rows
        if current_chunk:
            write_chunk(current_chunk, is_first_chunk)
    
    # Process in parallel mode with chunked writing
    else:
        print(f"Processing {total_rows} rows in parallel with {max_workers} workers...")
        is_first_chunk = True
        
        # Create chunks of the input dataframe to process in batches
        chunk_count = (total_rows + chunk_size - 1) // chunk_size
        
        for chunk_idx in range(chunk_count):
            start_idx = chunk_idx * chunk_size
            end_idx = min(start_idx + chunk_size, total_rows)
            
            print(f"Processing chunk {chunk_idx+1}/{chunk_count} (rows {start_idx+1}-{end_idx})")
            
            # Get current chunk of data
            chunk_df = input_df.iloc[start_idx:end_idx]
            
            # Prepare arguments for parallel processing
            args_list = [(idx, row, metrics_index, mcr_index) 
                         for idx, row in enumerate(chunk_df.to_dict('records'), start=start_idx)]
            
            # Process this chunk in parallel
            results = []
            with ProcessPoolExecutor(max_workers=max_workers) as executor:
                for result in executor.map(process_row_optimized, args_list):
                    if result:
                        results.append(result)
            
            # Write results from this chunk
            if results:
                write_chunk(results, is_first_chunk)
                is_first_chunk = False
    
    elapsed_time = time.time() - start_time
    print(f"Completed in {elapsed_time:.2f} seconds.")

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Gather contextual metrics for microservices.')
    parser.add_argument('input_csv', help='Path to input CSV file')
    parser.add_argument('--chunk-size', type=int, default=1000, 
                        help='Number of rows to process before writing to CSV')
    parser.add_argument('--sequential', action='store_true',
                        help='Force sequential processing (no parallelism)')
    parser.add_argument('--max-workers', type=int, default=None,
                        help='Maximum number of worker processes (default: auto)')
    
    args = parser.parse_args()
    
    print("\nCONTEXTUAL METRICS GATHERING TOOL (OPTIMIZED VERSION)")
    print(f"Input CSV: {args.input_csv}")
    print(f"Output Directory: output/")
    print(f"Using 10-minute search window (5 intervals before and after)")
    print(f"Using pre-built indexes from output/data/MSMetrics/index.pkl and output/data/MSRTMCR/index.pkl")
    print(f"Chunk size: {args.chunk_size} rows")
    print(f"Parallel processing: {'No' if args.sequential else 'Yes'}")
    
    process_input_csv_optimized(
        args.input_csv, 
        chunk_size=args.chunk_size,
        use_parallel=not args.sequential,
        max_workers=args.max_workers
    )

if __name__ == "__main__":
    main()