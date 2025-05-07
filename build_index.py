#!/usr/bin/env python3
import os
import pandas as pd
import pickle
import argparse
import time

def build_msmetrics_index(folder_path):
    """Build an index for MSMetrics folder with pre-aligned timestamps."""
    print(f"Building index for MSMetrics in {folder_path}...")
    start_time = time.time()
    
    # Structure: {timestamp: {msname: [record1, record2, ...]}}
    metrics_index = {}
    file_count = 0
    record_count = 0
    
    for filename in os.listdir(folder_path):
        if filename.endswith('.csv'):
            file_path = os.path.join(folder_path, filename)
            try:
                # Read only necessary columns to save memory
                df = pd.read_csv(file_path, usecols=['timestamp', 'msname', 'cpu_utilization', 'memory_utilization'])
                file_count += 1
                
                # Process each row
                for _, row in df.iterrows():
                    # Use timestamp directly (already aligned to 60s intervals)
                    timestamp = row['timestamp']
                    msname = row['msname']
                    
                    # Initialize nested dictionaries if needed
                    if timestamp not in metrics_index:
                        metrics_index[timestamp] = {}
                    if msname not in metrics_index[timestamp]:
                        metrics_index[timestamp][msname] = []
                    
                    # Add record to index
                    metrics_index[timestamp][msname].append({
                        'timestamp': timestamp,
                        'cpu_utilization': row.get('cpu_utilization'),
                        'memory_utilization': row.get('memory_utilization')
                    })
                    record_count += 1
                
                # Print progress every 10 files
                if file_count % 10 == 0:
                    print(f"Processed {file_count} files, {record_count} records so far...")
                    
            except Exception as e:
                print(f"Error processing {filename}: {str(e)}")
    
    # Save index to pickle file
    index_path = os.path.join(folder_path, "index.pkl")
    with open(index_path, 'wb') as f:
        pickle.dump(metrics_index, f)
    
    elapsed_time = time.time() - start_time
    print(f"Finished building MSMetrics index in {elapsed_time:.2f} seconds")
    print(f"Saved index to {index_path}")
    print(f"- Total time intervals: {len(metrics_index)}")
    print(f"- Total files processed: {file_count}")
    print(f"- Total records indexed: {record_count}")
    
    return metrics_index

def build_msrtmcr_index(folder_path):
    """Build an index for MSRTMCR folder with pre-aligned timestamps."""
    print(f"Building index for MSRTMCR in {folder_path}...")
    start_time = time.time()
    
    # Structure: {timestamp: {msname: [record1, record2, ...]}}
    mcr_index = {}
    file_count = 0
    record_count = 0
    
    for filename in os.listdir(folder_path):
        if filename.endswith('.csv'):
            file_path = os.path.join(folder_path, filename)
            try:
                # Read only necessary columns to save memory
                df = pd.read_csv(file_path, usecols=['timestamp', 'msname', 'providerrpc_mcr'])
                file_count += 1
                
                # Process each row
                for _, row in df.iterrows():
                    # Use timestamp directly (already aligned to 60s intervals)
                    timestamp = row['timestamp']
                    msname = row['msname']
                    
                    # Initialize nested dictionaries if needed
                    if timestamp not in mcr_index:
                        mcr_index[timestamp] = {}
                    if msname not in mcr_index[timestamp]:
                        mcr_index[timestamp][msname] = []
                    
                    # Add record to index
                    mcr_index[timestamp][msname].append({
                        'timestamp': timestamp,
                        'mcr': row.get('providerrpc_mcr')
                    })
                    record_count += 1
                
                # Print progress every 10 files
                if file_count % 10 == 0:
                    print(f"Processed {file_count} files, {record_count} records so far...")
                    
            except Exception as e:
                print(f"Error processing {filename}: {str(e)}")
    
    # Save index to pickle file
    index_path = os.path.join(folder_path, "index.pkl")
    with open(index_path, 'wb') as f:
        pickle.dump(mcr_index, f)
    
    elapsed_time = time.time() - start_time
    print(f"Finished building MSRTMCR index in {elapsed_time:.2f} seconds")
    print(f"Saved index to {index_path}")
    print(f"- Total time intervals: {len(mcr_index)}")
    print(f"- Total files processed: {file_count}")
    print(f"- Total records indexed: {record_count}")
    
    return mcr_index

def test_index(index_type, index_path):
    """Test the created index to ensure it's working as expected."""
    print(f"\nTesting {index_type} index at {index_path}...")
    
    try:
        with open(index_path, 'rb') as f:
            index = pickle.load(f)
        
        # Get some statistics
        total_intervals = len(index)
        if total_intervals == 0:
            print("ERROR: Index is empty!")
            return False
        
        # Test first interval
        first_interval = list(index.keys())[0]
        total_services = len(index[first_interval])
        if total_services == 0:
            print(f"WARNING: No services found in first interval {first_interval}")
        else:
            first_service = list(index[first_interval].keys())[0]
            total_records = len(index[first_interval][first_service])
            print(f"Sample interval {first_interval} has {total_services} services")
            print(f"Service '{first_service}' has {total_records} records")
            
            # Show a sample record
            sample_record = index[first_interval][first_service][0]
            print(f"Sample record: {sample_record}")
        
        print(f"Index test successful: {total_intervals} intervals found")
        return True
        
    except Exception as e:
        print(f"Error testing index: {str(e)}")
        return False

def main():
    parser = argparse.ArgumentParser(description='Build optimized indexes for microservice metrics lookup')
    parser.add_argument('--base-path', default='output/data', 
                        help='Base path containing MSMetrics and MSRTMCR folders')
    parser.add_argument('--msmetrics-only', action='store_true',
                        help='Build only MSMetrics index')
    parser.add_argument('--msrtmcr-only', action='store_true',
                        help='Build only MSRTMCR index')
    
    args = parser.parse_args()
    
    print("INDEX BUILDER TOOL")
    print(f"Base Path: {args.base_path}")
    print("Using pre-aligned timestamps (60-second intervals)")
    
    # Build MSMetrics index
    if not args.msrtmcr_only:
        msmetrics_path = os.path.join(args.base_path, 'MSMetrics')
        if os.path.exists(msmetrics_path):
            build_msmetrics_index(msmetrics_path)
            test_index("MSMetrics", os.path.join(msmetrics_path, "index.pkl"))
        else:
            print(f"Error: MSMetrics folder not found at {msmetrics_path}")
    
    # Build MSRTMCR index
    if not args.msmetrics_only:
        msrtmcr_path = os.path.join(args.base_path, 'MSRTMCR')
        if os.path.exists(msrtmcr_path):
            build_msrtmcr_index(msrtmcr_path)
            test_index("MSRTMCR", os.path.join(msrtmcr_path, "index.pkl"))
        else:
            print(f"Error: MSRTMCR folder not found at {msrtmcr_path}")
    
    print("\nIndex building completed!")

if __name__ == "__main__":
    main()