import pandas as pd
import csv
from collections import defaultdict
import time
import os
import glob
import numpy as np
import psutil  # For memory tracking

# Set output directory explicitly
# Set output directory explicitly
OUTPUT_DIR = "C:\\Users\\Tony\\Desktop\\1\\data"
print(f"Output directory set to: {OUTPUT_DIR}")

# Check directory permissions and create test file
try:
    test_file = os.path.join(OUTPUT_DIR, "test_file_access.txt")
    print(f"Testing file system access with: {test_file}")
    
    with open(test_file, 'w') as f:
        f.write("Test file access\n")
    
    if os.path.exists(test_file):
        print(f"SUCCESS: Test file created successfully at {test_file}")
        # Read it back to verify
        with open(test_file, 'r') as f:
            content = f.read()
            print(f"Successfully read back test file: {content.strip()}")
        # Remove the test file
        os.remove(test_file)
        print(f"Test file removed successfully")
    else:
        print(f"ERROR: Failed to create test file at {test_file}")
except Exception as e:
    print(f"ERROR with file system access: {e}")
    # Try another location
    try:
        OUTPUT_DIR = os.getcwd()  # Use current directory instead
        print(f"Falling back to current directory: {OUTPUT_DIR}")
        test_file = os.path.join(OUTPUT_DIR, "test_file_access.txt")
        with open(test_file, 'w') as f:
            f.write("Test file access\n")
        os.remove(test_file)
        print(f"SUCCESS: Current directory is writable, using it instead")
    except Exception as e2:
        print(f"ERROR with fallback directory: {e2}")
        print("CRITICAL: Cannot find a writable directory!")


def load_all_csvs_from_folder(folder_path, file_prefix):
    """Load all CSV files with given prefix from folder and concatenate them."""
    # Handle different naming patterns based on folder/prefix
    if "MSRTMCR" in folder_path:
        # For MSRTMCR folder with MCRRTUpdate_X.csv pattern
        all_files = glob.glob(os.path.join(folder_path, "MCRRTUpdate_*.csv"))
        print(f"Looking for MSRTMCR files with pattern: MCRRTUpdate_*.csv")
    else:
        # Standard pattern for other folders
        all_files = glob.glob(os.path.join(folder_path, f"{file_prefix}_*.csv"))
        print(f"Looking for files with pattern: {file_prefix}_*.csv")
    
    df_list = []
    
    print(f"Found {len(all_files)} files in {folder_path}")
    
    total_rows = 0
    for i, file in enumerate(all_files):
        try:
            print(f"Loading file {i+1}/{len(all_files)}: {os.path.basename(file)}...")
            # Using engine='python' and on_bad_lines='skip' to handle inconsistent rows
            df = pd.read_csv(file, engine='python', on_bad_lines='skip')
            df_list.append(df)
            total_rows += len(df)
            print(f"  Successfully loaded {len(df)} rows")
            
            # Print memory usage after loading each file
            memory_info = psutil.Process(os.getpid()).memory_info()
            print(f"  Current memory usage: {memory_info.rss / 1024 / 1024:.2f} MB")
        except Exception as e:
            print(f"  ERROR loading {file}: {e}")
    
    if not df_list:
        print(f"No valid files found for {file_prefix}")
        return None
    
    print(f"Concatenating {len(df_list)} dataframes...")
    concatenated_df = pd.concat(df_list, ignore_index=True)
    print(f"Total loaded: {len(concatenated_df)} rows for {file_prefix} (expected: {total_rows})")
    
    # Print column names for debugging
    print(f"Columns in the loaded dataframe: {concatenated_df.columns.tolist()}")
    
    return concatenated_df

def find_closest_record(df, timestamp, time_col='timestamp'):
    """Find the record with the closest timestamp."""
    if df is None or df.empty:
        return None
    
    # Calculate absolute difference from target timestamp
    df = df.copy()
    df['time_diff'] = (df[time_col] - timestamp).abs()
    
    # Get index of minimum difference
    min_idx = df['time_diff'].idxmin()
    
    # Return the row as a dictionary
    return df.loc[min_idx].to_dict()

def find_parent_call_rate(df, timestamp, parent_service, time_col='timestamp', service_col='msname'):
    """Find the call rate for a specific parent service at closest timestamp."""
    if df is None or df.empty:
        return None
    
    # Print available columns for debugging if parent_service isn't found
    if parent_service not in df[service_col].values:
        print(f"WARNING: Parent service '{parent_service}' not found in {service_col} column.")
        print(f"Available service names: {df[service_col].unique()[:10]} ... (showing first 10)")
        
        # Try alternative matching if exact match fails
        matches = [s for s in df[service_col].unique() if parent_service in s]
        if matches:
            print(f"Found potential matches: {matches}")
            parent_service = matches[0]
            print(f"Using '{parent_service}' as the parent service instead")
    
    # Filter for only the parent service
    parent_df = df[df[service_col] == parent_service].copy()
    
    if parent_df.empty:
        return None
    
    # Find closest timestamp record
    closest_record = find_closest_record(parent_df, timestamp, time_col)
    print(f"Found call rate record for {parent_service} at timestamp {timestamp}: {closest_record.get('providerRPC_MCR', 'N/A')}")
    return closest_record

def identify_sibling_relationships(call_graph_df):
    """Identify sibling relationships from call graph data."""
    print("Identifying sibling relationships...")
    start_time = time.time()
    
    # Initialize dictionaries
    trace_parent_map = defaultdict(lambda: defaultdict(set))
    
    # Check if required columns exist
    required_columns = ['traceid', 'rpc_id', 'um', 'dm']
    missing_columns = [col for col in required_columns if col not in call_graph_df.columns]
    
    if missing_columns:
        print(f"WARNING: Missing required columns: {missing_columns}")
        print("Available columns:", call_graph_df.columns.tolist())
        return []
    
    # Process each row
    total_rows = len(call_graph_df)
    print(f"Processing {total_rows} rows to identify siblings...")
    
    # Count valid and invalid rows
    valid_rpc_count = 0
    invalid_rpc_count = 0
    
    for idx, row in call_graph_df.iterrows():
        if idx % 100000 == 0:
            elapsed = time.time() - start_time
            rows_per_second = idx / elapsed if elapsed > 0 else 0
            eta_seconds = (total_rows - idx) / rows_per_second if rows_per_second > 0 else "unknown"
            if isinstance(eta_seconds, str):
                eta_str = eta_seconds
            else:
                eta_str = f"{eta_seconds:.2f}s ({eta_seconds/60:.2f}m)"
            
            print(f"Processed {idx:,}/{total_rows:,} rows ({idx/total_rows*100:.1f}%)... "
                  f"Speed: {rows_per_second:.1f} rows/sec, ETA: {eta_str}")
        
        traceid = row['traceid']
        rpc_id = row['rpc_id']
        um = row['um']
        dm = row['dm']
        
        # Skip rows with invalid rpc_id
        if pd.isna(rpc_id) or not str(rpc_id).strip():
            invalid_rpc_count += 1
            continue
        
        valid_rpc_count += 1
        
        # Parse rpc_id to get parent prefix and current segment
        rpc_id_str = str(rpc_id)
        segments = rpc_id_str.split('.')
        
        # Skip if rpc_id doesn't have a valid format
        if len(segments) < 1:
            continue
            
        # For siblings, we're interested in services that share the same parent
        parent_prefix = '.'.join(segments[:-1]) if len(segments) > 1 else ''
        
        # Store service in appropriate trace and parent - use combination of parent_prefix and um
        context_key = f"{parent_prefix}|{um}"
        trace_parent_map[traceid][context_key].add(dm)
    
    print(f"Row processing complete. Valid RPC IDs: {valid_rpc_count:,}, Invalid: {invalid_rpc_count:,}")
    
    # Identify sibling pairs
    print("Grouping sibling pairs...")
    sibling_pairs = set()
    trace_count = 0
    total_traces = len(trace_parent_map)
    
    trace_processing_start = time.time()
    for traceid in trace_parent_map:
        trace_count += 1
        if trace_count % 10000 == 0:
            elapsed = time.time() - trace_processing_start
            traces_per_second = trace_count / elapsed if elapsed > 0 else 0
            eta_seconds = (total_traces - trace_count) / traces_per_second if traces_per_second > 0 else "unknown"
            if isinstance(eta_seconds, str):
                eta_str = eta_seconds
            else:
                eta_str = f"{eta_seconds:.2f}s ({eta_seconds/60:.2f}m)"
                
            print(f"Processing trace {trace_count:,}/{total_traces:,} ({trace_count/total_traces*100:.1f}%)... "
                  f"Speed: {traces_per_second:.1f} traces/sec, ETA: {eta_str}")
            
        for context_key in trace_parent_map[traceid]:
            children = list(trace_parent_map[traceid][context_key])
            
            # Generate all pairs of siblings
            for i in range(len(children)):
                for j in range(i+1, len(children)):
                    # Include the context in the pair to track the specific request context
                    # Format: (service1, service2, parent_prefix, um)
                    parent_prefix, um = context_key.split('|', 1)
                    
                    sibling_pair = (children[i], children[j], parent_prefix, um)
                    sibling_pairs.add(sibling_pair)
    
    # Convert to list for easier handling
    sibling_pairs_list = list(sibling_pairs)
    print(f"Identified {len(sibling_pairs_list):,} sibling pairs across {total_traces:,} traces")
    
    # Print sample of sibling pairs for verification
    if sibling_pairs_list:
        print("Sample sibling pairs (first 5):")
        for i, pair in enumerate(sibling_pairs_list[:5]):
            s1, s2, prefix, parent = pair
            print(f"  {i+1}. Services: '{s1}' and '{s2}', Parent: '{parent}', Prefix: '{prefix}'")
    
    return sibling_pairs_list

def extract_timing_information(call_graph_df, sibling_pairs, system_load_df=None, call_rate_df=None):
    """Extract timing information and contextual data for sibling pairs."""
    print("Extracting timing information...")
    
    # Process sibling pairs in batch to avoid memory issues
    batch_size = 5000  # Process 5000 pairs at a time
    num_batches = (len(sibling_pairs) + batch_size - 1) // batch_size  # Ceiling division
    
    print(f"Processing {len(sibling_pairs):,} pairs in {num_batches} batches of size {batch_size}")
    
    # Create a structure to store execution patterns
    all_execution_patterns = []
    
    for batch_num in range(num_batches):
        start_idx = batch_num * batch_size
        end_idx = min(start_idx + batch_size, len(sibling_pairs))
        
        print(f"Processing batch {batch_num+1}/{num_batches} (pairs {start_idx+1:,} to {end_idx:,})")
        
        # Memory check before batch processing
        memory_info = psutil.Process(os.getpid()).memory_info()
        print(f"Memory usage before batch {batch_num+1}: {memory_info.rss / 1024 / 1024:.2f} MB")
        
        # Get the pairs for this batch
        batch_pairs = sibling_pairs[start_idx:end_idx]
        
        # Create a set of services we're interested in for this batch
        services_of_interest = set()
        for s1, s2, _, _ in batch_pairs:
            services_of_interest.add(s1)
            services_of_interest.add(s2)
        
        print(f"Looking for timing information for {len(services_of_interest)} unique services in this batch")
        
        # Create a dictionary to store service timing information for this batch
        service_timing = defaultdict(lambda: defaultdict(list))
        
        # Process each row to collect timing information
        total_rows = len(call_graph_df)
        timing_rows_processed = 0
        timing_rows_matched = 0
        zero_rt_count = 0
        
        timing_start_time = time.time()
        
        print(f"Scanning {total_rows:,} rows for timing information...")
        for idx, row in call_graph_df.iterrows():
            timing_rows_processed += 1
            
            if idx % 500000 == 0 and idx > 0:
                elapsed = time.time() - timing_start_time
                rows_per_second = idx / elapsed if elapsed > 0 else 0
                eta_seconds = (total_rows - idx) / rows_per_second if rows_per_second > 0 else "unknown"
                if isinstance(eta_seconds, str):
                    eta_str = eta_seconds
                else:
                    eta_str = f"{eta_seconds:.2f}s ({eta_seconds/60:.2f}m)"
                
                print(f"Scanning row {idx:,}/{total_rows:,} ({idx/total_rows*100:.1f}%)... "
                      f"Matched: {timing_rows_matched:,}, Zero RT: {zero_rt_count:,}, "
                      f"Speed: {rows_per_second:.1f} rows/sec, ETA: {eta_str}")
            
            traceid = row['traceid']
            dm = row['dm']
            
            # Skip if this service is not part of our batch
            if dm not in services_of_interest:
                continue
            
            # Extract timing information
            timestamp = row['timestamp']
            rt = row['rt']
            
            # Skip invalid timing data
            if pd.isna(timestamp) or pd.isna(rt):
                continue
                
            # Count but don't skip if response time is 0 (might be useful for analysis)
            if rt == 0:
                zero_rt_count += 1
                # Add a small value instead of zero to avoid start = end
                # This is a heuristic - we assume the service took some minimal time
                rt = 0.001  # 1 microsecond as a minimal duration
            
            timing_rows_matched += 1
            
            # Calculate start and end times
            start_time = timestamp
            end_time = timestamp + rt
            
            # Store timing information for each service in each trace
            service_timing[traceid][dm].append((start_time, end_time))
        
        print(f"Timing scan complete. Matched {timing_rows_matched:,} rows, "
              f"Zero RT values adjusted: {zero_rt_count:,}")
        
        # Analyze execution patterns for sibling pairs in this batch
        batch_execution_patterns = []
        pair_count = 0
        total_pairs = len(batch_pairs)
        
        print(f"Analyzing execution patterns for {total_pairs:,} sibling pairs...")
        batch_start_time = time.time()
        
        for s1, s2, parent_prefix, um in batch_pairs:
            pair_count += 1
            if pair_count % 100 == 0:
                elapsed = time.time() - batch_start_time
                pairs_per_second = pair_count / elapsed if elapsed > 0 else 0
                eta_seconds = (total_pairs - pair_count) / pairs_per_second if pairs_per_second > 0 else "unknown"
                if isinstance(eta_seconds, str):
                    eta_str = eta_seconds
                else:
                    eta_str = f"{eta_seconds:.2f}s ({eta_seconds/60:.2f}m)"
                
                print(f"Analyzing pair {pair_count:,}/{total_pairs:,} ({pair_count/total_pairs*100:.1f}%)... "
                      f"Speed: {pairs_per_second:.2f} pairs/sec, ETA: {eta_str}")
            
            concurrent_count = 0
            sequential_s1_s2_count = 0
            sequential_s2_s1_count = 0
            total_observations = 0
            
            # Store contextual information for each observation
            observations = []
            
            # Check if either service has no timing data
            if s1 not in service_timing or s2 not in service_timing:
                if pair_count % 1000 == 0:
                    print(f"  Skipping pair ({s1}, {s2}) - no timing data available")
                continue
                
            # Find traces where both services appear
            common_traces = set(service_timing[s1].keys()) & set(service_timing[s2].keys())
            
            if not common_traces:
                if pair_count % 1000 == 0:
                    print(f"  Skipping pair ({s1}, {s2}) - no common traces")
                continue
                
            # Check all traces where both services appear
            for traceid in common_traces:
                # Get all timing instances for both services
                s1_timings = service_timing[traceid][s1]
                s2_timings = service_timing[traceid][s2]
                
                # If we have multiple executions of the same service in a trace,
                # this creates multiple combinations to evaluate
                combinations = 0
                for s1_start, s1_end in s1_timings:
                    for s2_start, s2_end in s2_timings:
                        combinations += 1
                        total_observations += 1
                        
                        # Determine execution pattern
                        if s1_end <= s2_start:
                            # Sequential: s1 -> s2
                            pattern = "sequential_s1_s2"
                            sequential_s1_s2_count += 1
                        elif s2_end <= s1_start:
                            # Sequential: s2 -> s1
                            pattern = "sequential_s2_s1"
                            sequential_s2_s1_count += 1
                        else:
                            # Concurrent
                            pattern = "concurrent"
                            concurrent_count += 1
                        
                        # Add observation with timestamp for potential contextual analysis
                        min_start = min(s1_start, s2_start)
                        observations.append({
                            "traceid": traceid,
                            "timestamp": min_start,
                            "pattern": pattern
                        })
                
                if combinations > 1 and pair_count % 1000 == 0:
                    print(f"  Trace {traceid} has {combinations} timing combinations for ({s1}, {s2})")
            
            # Skip pairs with no observations
            if total_observations == 0:
                if pair_count % 1000 == 0:
                    print(f"  Skipping pair ({s1}, {s2}) - no valid observations")
                continue
                
            # Get contextual information if available
            context_info = {
                'system_load_stats': {},
                'call_rate_stats': {}
            }
            
            if system_load_df is not None or call_rate_df is not None:
                context_info = collect_contextual_stats(
                    observations, 
                    um,  # Use the parent service name from the sibling pair tuple
                    system_load_df, 
                    call_rate_df
                )
            
            # Calculate percentages
            p_concurrent = concurrent_count / total_observations
            p_sequential_s1_s2 = sequential_s1_s2_count / total_observations
            p_sequential_s2_s1 = sequential_s2_s1_count / total_observations
            
            pattern_data = {
                'sibling_pair': (s1, s2),
                'parent_prefix': parent_prefix,
                'um': um,
                'total_observations': total_observations,
                'concurrent_count': concurrent_count,
                'sequential_s1_s2_count': sequential_s1_s2_count,
                'sequential_s2_s1_count': sequential_s2_s1_count,
                'p_concurrent': p_concurrent,
                'p_sequential_s1_s2': p_sequential_s1_s2,
                'p_sequential_s2_s1': p_sequential_s2_s1
            }
            
            # Add contextual information
            pattern_data.update(context_info)
            
            batch_execution_patterns.append(pattern_data)
        
        # Add batch results to overall results
        all_execution_patterns.extend(batch_execution_patterns)
        
        # Save intermediate results for this batch - with explicit path
        try:
            batch_output_file = os.path.join(OUTPUT_DIR, f"sibling_analysis_batch_{batch_num+1}.csv")
            save_results_to_csv(batch_execution_patterns, batch_output_file)
            print(f"Batch {batch_num+1} results saved to {batch_output_file}")
            
            # Verify the file was created
            if os.path.exists(batch_output_file):
                print(f"Verified: File exists at {batch_output_file}")
                print(f"File size: {os.path.getsize(batch_output_file) / 1024:.1f} KB")
            else:
                print(f"WARNING: File {batch_output_file} was not created!")
                
        except Exception as e:
            print(f"Error saving batch results: {e}")
        
        # Memory check after batch processing
        memory_info = psutil.Process(os.getpid()).memory_info()
        print(f"Memory usage after batch {batch_num+1}: {memory_info.rss / 1024 / 1024:.2f} MB")
    
    # Print overall statistics
    print(f"Total execution patterns analyzed: {len(all_execution_patterns):,}")
    print(f"Distinct sibling pairs analyzed: {len(set((p['sibling_pair'][0], p['sibling_pair'][1]) for p in all_execution_patterns)):,}")
    
    # Count patterns by type
    if all_execution_patterns:
        always_concurrent = sum(1 for p in all_execution_patterns if p['p_concurrent'] == 1.0)
        mostly_concurrent = sum(1 for p in all_execution_patterns if 0.7 <= p['p_concurrent'] < 1.0)
        mixed = sum(1 for p in all_execution_patterns if 0.3 <= p['p_concurrent'] < 0.7)
        mostly_sequential = sum(1 for p in all_execution_patterns if 0 <= p['p_concurrent'] < 0.3)
        
        print(f"Pattern distribution:")
        print(f"  Always concurrent (100%): {always_concurrent:,} pairs")
        print(f"  Mostly concurrent (70-99%): {mostly_concurrent:,} pairs")
        print(f"  Mixed patterns (30-70%): {mixed:,} pairs")
        print(f"  Mostly sequential (0-30%): {mostly_sequential:,} pairs")
    
    return all_execution_patterns

def collect_contextual_stats(observations, parent_service, system_load_df, call_rate_df):
    """Collect statistics on execution patterns by system load and call rate."""
    # Initialize counters
    system_load_stats = {
        'high': {'concurrent': 0, 'sequential': 0, 'total': 0},
        'medium': {'concurrent': 0, 'sequential': 0, 'total': 0},
        'low': {'concurrent': 0, 'sequential': 0, 'total': 0}
    }
    
    call_rate_stats = {
        'high': {'concurrent': 0, 'sequential': 0, 'total': 0},
        'medium': {'concurrent': 0, 'sequential': 0, 'total': 0},
        'low': {'concurrent': 0, 'sequential': 0, 'total': 0}
    }
    
    # Skip if no observations or no contextual data
    if not observations or (system_load_df is None and call_rate_df is None):
        return {
            'system_load_stats': system_load_stats,
            'call_rate_stats': call_rate_stats
        }
    
    # Categorize system load thresholds
    load_thresholds = {
        'low': 0.3,    # < 30% utilization
        'medium': 0.7  # 30-70% utilization
        # high is > 70%
    }
    
    # If we have call rate data, calculate percentiles for thresholds
    call_rate_thresholds = None
    if call_rate_df is not None and 'providerRPC_MCR' in call_rate_df.columns:
        call_rates = call_rate_df['providerRPC_MCR'].dropna()
        if not call_rates.empty:
            call_rate_thresholds = {
                'low': np.percentile(call_rates, 25),      # < 25th percentile
                'medium': np.percentile(call_rates, 75)    # 25th-75th percentile
                # high is > 75th percentile
            }
            print(f"Call rate thresholds calculated: Low < {call_rate_thresholds['low']:.2f}, "
                  f"Medium: {call_rate_thresholds['low']:.2f}-{call_rate_thresholds['medium']:.2f}, "
                  f"High > {call_rate_thresholds['medium']:.2f}")
    
    # Process each observation
    observations_processed = 0
    total_observations = len(observations)
    context_start_time = time.time()
    
    for obs in observations:
        observations_processed += 1
        if observations_processed % 10000 == 0:
            elapsed = time.time() - context_start_time
            obs_per_second = observations_processed / elapsed if elapsed > 0 else 0
            eta_seconds = (total_observations - observations_processed) / obs_per_second if obs_per_second > 0 else "unknown"
            if isinstance(eta_seconds, str):
                eta_str = eta_seconds
            else:
                eta_str = f"{eta_seconds:.2f}s ({eta_seconds/60:.2f}m)"
                
            print(f"Processing observation {observations_processed:,}/{total_observations:,} "
                  f"({observations_processed/total_observations*100:.1f}%)... "
                  f"Speed: {obs_per_second:.1f} obs/sec, ETA: {eta_str}")
        
        timestamp = obs['timestamp']
        pattern = obs['pattern']
        is_concurrent = pattern == 'concurrent'
        
        # System load analysis
        if system_load_df is not None:
            try:
                # Find closest timestamp in system_load_df
                closest_load = find_closest_record(system_load_df, timestamp)
                
                if closest_load is not None:
                    # Calculate weighted system load
                    system_load = (0.7 * closest_load.get('cpu_utilization', 0) + 
                                  0.3 * closest_load.get('memory_utilization', 0))
                    
                    # Categorize the load
                    load_category = 'high'
                    if system_load < load_thresholds['low']:
                        load_category = 'low'
                    elif system_load < load_thresholds['medium']:
                        load_category = 'medium'
                    
                    # Increment appropriate counter
                    if is_concurrent:
                        system_load_stats[load_category]['concurrent'] += 1
                    else:
                        system_load_stats[load_category]['sequential'] += 1
                        
                    system_load_stats[load_category]['total'] += 1
            except Exception as e:
                print(f"Error processing system load data: {e}")
        
        # Call rate analysis
        if call_rate_df is not None and parent_service and call_rate_thresholds is not None:
            try:
                # Find call rate for parent service at closest timestamp
                # Using 'msname' as service column, might need adjustment based on actual data
                closest_rate = find_parent_call_rate(
                    call_rate_df, 
                    timestamp, 
                    parent_service,
                    service_col='msname'  # Adjust based on your actual column name
                )
                
                if closest_rate is not None:
                    call_rate = closest_rate.get('providerRPC_MCR', 0)
                    
                    # Categorize the call rate
                    rate_category = 'high'
                    if call_rate < call_rate_thresholds['low']:
                        rate_category = 'low'
                    elif call_rate < call_rate_thresholds['medium']:
                        rate_category = 'medium'
                    
                    # Increment appropriate counter
                    if is_concurrent:
                        call_rate_stats[rate_category]['concurrent'] += 1
                    else:
                        call_rate_stats[rate_category]['sequential'] += 1
                        
                    call_rate_stats[rate_category]['total'] += 1
            except Exception as e:
                print(f"Error processing call rate data: {e}")
    
    # Calculate percentages for each category
    for category in system_load_stats:
        if system_load_stats[category]['total'] > 0:
            system_load_stats[category]['p_concurrent'] = system_load_stats[category]['concurrent'] / system_load_stats[category]['total']
            system_load_stats[category]['p_sequential'] = system_load_stats[category]['sequential'] / system_load_stats[category]['total']
    
    for category in call_rate_stats:
        if call_rate_stats[category]['total'] > 0:
            call_rate_stats[category]['p_concurrent'] = call_rate_stats[category]['concurrent'] / call_rate_stats[category]['total']
            call_rate_stats[category]['p_sequential'] = call_rate_stats[category]['sequential'] / call_rate_stats[category]['total']
    
    return {
        'system_load_stats': system_load_stats,
        'call_rate_stats': call_rate_stats
    }

def save_results_to_csv(execution_patterns, filename):
    """Save execution patterns to a CSV file."""
    if not execution_patterns:
        print(f"No data to save to {filename}")
        # Create an empty file with headers to confirm file creation works
        try:
            with open(filename, 'w', newline='') as f:
                f.write("# No valid execution patterns found in this batch\n")
                f.write("# Empty file created to confirm file system access\n")
            print(f"Created empty file at {filename}")
            if os.path.exists(filename):
                print(f"Verified: Empty file exists at {filename}")
            else:
                print(f"ERROR: Failed to create even empty file at {filename}")
        except Exception as e:
            print(f"ERROR creating empty file: {e}")
        return
    
    print(f"Saving {len(execution_patterns):,} patterns to {filename}...")
    
    with open(filename, 'w', newline='') as f:
        # Get all possible column names from the first result
        flat_dict = {}
        for key, value in execution_patterns[0].items():
            if isinstance(value, dict):
                # Flatten nested dictionaries with prefix
                for sub_key, sub_value in value.items():
                    if isinstance(sub_value, dict):
                        for sub_sub_key, sub_sub_value in sub_value.items():
                            flat_dict[f"{key}_{sub_key}_{sub_sub_key}"] = sub_sub_value
                    else:
                        flat_dict[f"{key}_{sub_key}"] = sub_value
            else:
                flat_dict[key] = value
        
        # Handle tuple fields
        if 'sibling_pair' in flat_dict:
            s1, s2 = flat_dict.pop('sibling_pair')
            flat_dict['Service1'] = s1
            flat_dict['Service2'] = s2
        
        fieldnames = list(flat_dict.keys())
        print(f"CSV columns: {fieldnames}")
        
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
        records_written = 0
        for pattern in execution_patterns:
            # Handle tuples and flatten nested dictionaries
            row_dict = {}
            for key, value in pattern.items():
                if key == 'sibling_pair':
                    s1, s2 = value
                    row_dict['Service1'] = s1
                    row_dict['Service2'] = s2
                elif isinstance(value, dict):
                    # Flatten nested dictionaries with prefix
                    for sub_key, sub_value in value.items():
                        if isinstance(sub_value, dict):
                            for sub_sub_key, sub_sub_value in sub_value.items():
                                row_dict[f"{key}_{sub_key}_{sub_sub_key}"] = sub_sub_value
                        else:
                            row_dict[f"{key}_{sub_key}"] = sub_value
                else:
                    row_dict[key] = value
            
            writer.writerow(row_dict)
            records_written += 1
            
            if records_written % 1000 == 0:
                print(f"  Wrote {records_written:,} records...")
        
        print(f"Successfully wrote {records_written:,} records to {filename}")

def analyze_context_specific_dependencies(execution_patterns):
    """Analyze patterns where services behave differently in different contexts."""
    print("\nAnalyzing context-specific dependencies...")
    start_time = time.time()
    
    # Group by service pairs across different parent prefixes
    service_pair_contexts = defaultdict(list)
    
    print(f"Processing {len(execution_patterns):,} execution patterns...")
    
    for pattern in execution_patterns:
        s1, s2 = pattern['sibling_pair']
        parent_prefix = pattern['parent_prefix']
        service_key = tuple(sorted([s1, s2]))  # Ensure consistent ordering
        
        service_pair_contexts[service_key].append({
            'parent_prefix': parent_prefix,
            'um': pattern['um'],
            'p_concurrent': pattern['p_concurrent'],
            'p_sequential_s1_s2': pattern['p_sequential_s1_s2'],
            'p_sequential_s2_s1': pattern['p_sequential_s2_s1'],
            'total_observations': pattern['total_observations']
        })
    
    # Print grouping statistics
    print(f"Found {len(service_pair_contexts):,} unique service pairs across all contexts")
    
    # Find pairs that appear in multiple contexts with different behaviors
    context_specific_pairs = []
    multi_context_count = 0
    
    for service_pair, contexts in service_pair_contexts.items():
        if len(contexts) > 1:
            multi_context_count += 1
            
            # Check if behavior varies significantly across contexts
            concurrent_rates = [ctx['p_concurrent'] for ctx in contexts]
            max_diff = max(concurrent_rates) - min(concurrent_rates)
            
            if max_diff > 0.4:  # 40% difference threshold
                context_specific_pairs.append({
                    'service_pair': service_pair,
                    'contexts': contexts,
                    'max_difference': max_diff
                })
    
    print(f"Found {multi_context_count:,} service pairs that appear in multiple contexts")
    print(f"Found {len(context_specific_pairs):,} service pairs with significant context-specific dependencies "
          f"(>40% difference in concurrency)")
    
    # Print results
    if context_specific_pairs:
        print("\nTop examples of context-specific dependencies:")
        
        # Sort by maximum difference in concurrent rates
        sorted_pairs = sorted(
            context_specific_pairs, 
            key=lambda x: x['max_difference'],
            reverse=True
        )
        
        for i, pair_data in enumerate(sorted_pairs[:5]):  # Show first 5 examples
            s1, s2 = pair_data['service_pair']
            contexts = pair_data['contexts']
            max_diff = pair_data['max_difference']
            
            print(f"\n{i+1}. {s1} and {s2} behavior varies by {max_diff*100:.1f}% across contexts:")
            
            for ctx in contexts:
                prefix = ctx['parent_prefix']
                parent = ctx['um']
                observations = ctx['total_observations']
                
                print(f"  • Context: {prefix or 'ROOT'} by {parent}:")
                print(f"    - Concurrent: {ctx['p_concurrent']*100:.1f}%, "
                      f"Sequential S1→S2: {ctx['p_sequential_s1_s2']*100:.1f}%, "
                      f"Sequential S2→S1: {ctx['p_sequential_s2_s1']*100:.1f}%")
                print(f"    - Based on {observations:,} observations")
        
        # Save context-specific pairs to CSV with explicit path
        try:
            output_file = os.path.join(OUTPUT_DIR, "context_specific_dependencies.csv")
            with open(output_file, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    'Service1', 'Service2', 
                    'Context', 'ParentService', 
                    'P_Concurrent', 'P_Sequential_S1_S2', 'P_Sequential_S2_S1', 
                    'Observations', 'Max_Difference'
                ])
                
                for pair_data in context_specific_pairs:
                    s1, s2 = pair_data['service_pair']
                    max_diff = pair_data['max_difference']
                    
                    for ctx in pair_data['contexts']:
                        writer.writerow([
                            s1, s2, 
                            ctx['parent_prefix'],
                            ctx['um'],
                            ctx['p_concurrent'],
                            ctx['p_sequential_s1_s2'],
                            ctx['p_sequential_s2_s1'],
                            ctx['total_observations'],
                            max_diff
                        ])
            
            print(f"\nContext-specific dependencies saved to {output_file}")
            
            # Verify file was created
            if os.path.exists(output_file):
                print(f"Verified: File exists at {output_file}")
                print(f"File size: {os.path.getsize(output_file) / 1024:.1f} KB")
            else:
                print(f"WARNING: File {output_file} was not created!")
                
        except Exception as e:
            print(f"Error saving context-specific dependencies: {e}")
    else:
        print("No significant context-specific dependencies found")

def main():
    # Start timing the entire process
    total_start_time = time.time()
    print(f"==== Starting sibling relationship analysis at {time.strftime('%Y-%m-%d %H:%M:%S')} ====")
    print(f"All output files will be saved to: {OUTPUT_DIR}")
    
    # Check if OUTPUT_DIR exists and is writable
    try:
        # Test write access by creating and removing a test file
        test_file = os.path.join(OUTPUT_DIR, "test_write_access.tmp")
        with open(test_file, 'w') as f:
            f.write("Testing write access")
        os.remove(test_file)
        print(f"Confirmed: Output directory {OUTPUT_DIR} exists and is writable")
    except Exception as e:
        print(f"WARNING: Output directory issue: {e}")
    
    # Base directory paths for each data type
    base_dir = "."  # Current directory, modify as needed
    print(f"Using base directory: {os.path.abspath(base_dir)}")
    
    call_graph_dir = os.path.join(base_dir, "CallGraph")
    ms_metrics_dir = os.path.join(base_dir, "MSMetrics")
    msrtmcr_dir = os.path.join(base_dir, "MSRTMCR")  # Note: Files named MCRRTUpdate_*.csv
    node_metrics_dir = os.path.join(base_dir, "NodeMetrics")
    
    # Check if directories exist
    for directory in [call_graph_dir, ms_metrics_dir, msrtmcr_dir, node_metrics_dir]:
        if os.path.exists(directory):
            print(f"Directory exists: {directory}")
        else:
            print(f"WARNING: Directory does not exist: {directory}")
    
    # Load data from each directory
    print("\n==== Loading data from all sources ====")
    
    # Load CallGraph data (required)
    print("\nLoading CallGraph data...")
    call_graph_df = load_all_csvs_from_folder(call_graph_dir, "CallGraph")
    if call_graph_df is None or call_graph_df.empty:
        print("ERROR: Could not load CallGraph data, which is required for analysis.")
        return
    
    print(f"CallGraph data loaded: {len(call_graph_df):,} rows, {call_graph_df.shape[1]} columns")
    
    # Load contextual data (optional)
    print("\nLoading MSMetrics data...")
    ms_metrics_df = load_all_csvs_from_folder(ms_metrics_dir, "MSMetricsUpdate")
    
    print("\nLoading MSRTMCR data...")
    # Note: In the MSRTMCR directory, files are named MCRRTUpdate_*.csv
    msrtmcr_df = load_all_csvs_from_folder(msrtmcr_dir, "MCRRTUpdate")
    
    print("\nLoading NodeMetrics data...")
    node_metrics_df = load_all_csvs_from_folder(node_metrics_dir, "NodeMetricsUpdate")
    
    # Check if we have enough contextual data
    has_system_load = node_metrics_df is not None and not node_metrics_df.empty
    has_call_rate = msrtmcr_df is not None and not msrtmcr_df.empty
    
    print(f"\nContextual data availability:")
    print(f"  System load data available: {has_system_load}")
    if has_system_load:
        print(f"    NodeMetrics rows: {len(node_metrics_df):,}")
    
    print(f"  Call rate data available: {has_call_rate}")
    if has_call_rate:
        print(f"    MSRTMCR rows: {len(msrtmcr_df):,}")
        
        # Check if msname column exists in MSRTMCR data
        if 'msname' in msrtmcr_df.columns:
            print(f"    MSRTMCR contains {msrtmcr_df['msname'].nunique():,} unique service names")
        else:
            print("    WARNING: MSRTMCR data doesn't have 'msname' column")
            print(f"    Available columns: {msrtmcr_df.columns.tolist()}")
    
    # Step 1: Identify sibling relationships
    print("\n==== Step 1: Identifying sibling relationships ====")
    sibling_pairs = identify_sibling_relationships(call_graph_df)
    if not sibling_pairs:
        print("ERROR: No sibling pairs found. Exiting.")
        return
    
    print(f"Found {len(sibling_pairs):,} sibling pairs")
    
    # Step 2: Analyze execution patterns with contextual data
    print("\n==== Step 2: Analyzing execution patterns with contextual data ====")
    execution_patterns = extract_timing_information(
        call_graph_df, 
        sibling_pairs,
        node_metrics_df if has_system_load else None,
        msrtmcr_df if has_call_rate else None
    )
    
    if not execution_patterns:
        print("ERROR: Could not extract timing information. Exiting.")
        return
    
    # Step 3: Classify pairs and summarize results
    print("\n==== Step 3: Classifying pairs and summarizing results ====")
    always_concurrent = []
    context_dependent = []
    
    for pattern in execution_patterns:
        if pattern['p_concurrent'] == 1.0:
            always_concurrent.append(pattern)
        else:
            context_dependent.append(pattern)
    
    # Print results
    print(f"Classification complete:")
    print(f"  Always concurrent pairs: {len(always_concurrent):,}")
    print(f"  Context-dependent pairs: {len(context_dependent):,}")
    
    # Print example context-dependent pairs
    if context_dependent:
        print("\nExample context-dependent pairs:")
        for i, pattern in enumerate(context_dependent[:5]):  # Show first 5 examples
            s1, s2 = pattern['sibling_pair']
            p_concurrent = pattern['p_concurrent']
            p_seq_s1_s2 = pattern['p_sequential_s1_s2']
            p_seq_s2_s1 = pattern['p_sequential_s2_s1']
            
            print(f"{i+1}. {s1} and {s2}: "
                  f"concurrent: {p_concurrent:.2f}, "
                  f"s1->s2: {p_seq_s1_s2:.2f}, "
                  f"s2->s1: {p_seq_s2_s1:.2f}")
    
    # Save final results with explicit path
    try:
        output_file = os.path.join(OUTPUT_DIR, "sibling_analysis_results.csv")
        save_results_to_csv(execution_patterns, output_file)
        print(f"\nFinal results saved to {output_file}")
        
        # Verify file was created
        if os.path.exists(output_file):
            print(f"Verified: File exists at {output_file}")
            print(f"File size: {os.path.getsize(output_file) / 1024:.1f} KB")
        else:
            print(f"WARNING: File {output_file} was not created!")
            
    except Exception as e:
        print(f"Error saving results: {e}")
    
    # Analyze context-specific patterns
    print("\n==== Step 4: Analyzing context-specific dependencies ====")
    analyze_context_specific_dependencies(execution_patterns)
    
    # Calculate and print total runtime
    total_runtime = time.time() - total_start_time
    hours, remainder = divmod(total_runtime, 3600)
    minutes, seconds = divmod(remainder, 60)
    
    print(f"\n==== Analysis complete! ====")
    print(f"Total runtime: {int(hours)}h {int(minutes)}m {int(seconds)}s")
    print(f"Results saved to:")
    print(f"  - {os.path.join(OUTPUT_DIR, 'sibling_analysis_results.csv')}")
    print(f"  - {os.path.join(OUTPUT_DIR, 'context_specific_dependencies.csv')}")
    print(f"  - {os.path.join(OUTPUT_DIR, 'sibling_analysis_batch_*.csv')} (intermediate results)")
    
    return {
        'sibling_pairs': sibling_pairs,
        'execution_patterns': execution_patterns,
        'always_concurrent': always_concurrent,
        'context_dependent': context_dependent
    }

if __name__ == "__main__":
    try:
        results = main()
        print("Program completed successfully")
    except Exception as e:
        print(f"ERROR: Program failed with exception: {e}")
        import traceback
        traceback.print_exc()