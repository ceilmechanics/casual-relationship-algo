#!/usr/bin/env python3
"""
Standalone CSV Filter for Microservice Metrics Data

This script filters CSV files in tar.gz archives based on a timestamp cutoff.
Updated to skip creating empty CSV files when filtered rows = 0.
"""

import pandas as pd
import os
import tarfile
import shutil
import re
import csv
import argparse
import sys
import time

class CSVFilter:
    """Utility class for filtering CSV files by timestamp"""
    
    @staticmethod
    def filter_csv_copy(csv_path, cutoff_timestamp, output_path):
        """Filter CSV file by timestamp and save to output path. Returns max timestamp in file."""
        try:
            print(f"    Processing: {os.path.basename(csv_path)}")
            print(f"      Input file:  {csv_path}")
            print(f"      Output file: {output_path}")
            print(f"      Cutoff:      {cutoff_timestamp}")
            
            # First, let's check if file exists and is readable
            if not os.path.exists(csv_path):
                print(f"      ERROR: File does not exist")
                return None
            
            # Create output directory if it doesn't exist
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # Read CSV file - let's be more robust about encoding
            try:
                df = pd.read_csv(csv_path)
            except Exception as e:
                print(f"      WARNING: Default encoding failed: {e}")
                print(f"      INFO: Trying alternative encodings...")
                encodings = ['utf-8', 'latin1', 'cp1252', 'iso-8859-1']
                df = None
                for encoding in encodings:
                    try:
                        df = pd.read_csv(csv_path, encoding=encoding)
                        print(f"        SUCCESS: Read with {encoding} encoding")
                        break
                    except Exception as enc_e:
                        print(f"        FAILED: {encoding} encoding: {enc_e}")
                
                if df is None:
                    print(f"      ERROR: Could not read CSV file with any encoding")
                    # Don't copy original file if we can't process it
                    print(f"      WARNING: Skipping file due to encoding issues")
                    return None
            
            print(f"      INFO: Columns: {df.columns.tolist()}")
            print(f"      INFO: Shape: {df.shape}")
            
            # Look for timestamp column (might have different names)
            timestamp_col = None
            for col in ['timestamp', 'ts', 'time', 'Timestamp', 'TS']:
                if col in df.columns:
                    timestamp_col = col
                    print(f"      Found timestamp column: '{timestamp_col}'")
                    break
            
            if timestamp_col is None:
                # No timestamp column found, skip this file
                print(f"      WARNING: No timestamp column found, skipping file")
                print(f"      RESULT: No timestamp column, file not created")
                return None
            
            original_rows = len(df)
            print(f"      INFO: Original rows: {original_rows}")
            
            # Calculate the cutoff row using vectorized operation
            df_filtered = df[df[timestamp_col] <= cutoff_timestamp]
            filtered_rows = len(df_filtered)
            print(f"      INFO: Filtered rows: {filtered_rows}")
            
            if filtered_rows > 0:
                max_timestamp = df_filtered[timestamp_col].max()
                print(f"      INFO: Max timestamp in filtered data: {max_timestamp}")
                # Save filtered data to output
                df_filtered.to_csv(output_path, index=False)
                print(f"      SUCCESS: Filtered data saved to output path")
            else:
                max_timestamp = None
                print(f"      WARNING: No data passed the filter")
                # Skip creating empty file - this is the key change
                print(f"      INFO: Skipping empty file creation (filtered rows = 0)")
                # Mark that output file should not be included in archive
                if os.path.exists(output_path):
                    os.remove(output_path)
            
            reduction = ((original_rows - filtered_rows) / original_rows * 100) if original_rows > 0 else 0
            print(f"      SUMMARY: {os.path.basename(csv_path)}: {original_rows} → {filtered_rows} rows " + \
                  f"(reduction: {reduction:.1f}%{', max_ts: ' + str(int(max_timestamp)) if max_timestamp else ''})")
            print()
            
            return max_timestamp
                
        except Exception as e:
            print(f"      ERROR: Exception processing {csv_path}: {e}")
            import traceback
            traceback.print_exc()
            # Don't copy original file on error
            print(f"      WARNING: Skipping file due to error")
            return None
    
    @staticmethod
    def process_metric_directory(input_dir, metric_type, cutoff_timestamp, output_dir=None):
        """Process ALL metric files from tar.gz archives and filter by cutoff timestamp"""
        print(f"\n{'='*60}")
        print(f" Processing Metric Type: {metric_type}")
        print(f"{'='*60}")
        print(f"  Input Directory:  {input_dir}")
        print(f"  Cutoff Timestamp: {cutoff_timestamp}")
        if output_dir:
            print(f"  Output Directory: {output_dir}")
        print()
        
        # Create output directory for this metric type
        if output_dir:
            metric_output_dir = os.path.join(output_dir, metric_type)
            os.makedirs(metric_output_dir, exist_ok=True)
        
        # Get all tar.gz files and sort them by the index
        tar_files = CSVFilter._get_sorted_tar_files(input_dir)
        print(f"  Found {len(tar_files)} tar.gz files")
        print()
        
        # Process each tar.gz file in order
        processed_files = 0
        total_files = len(tar_files)
        total_csv_files = 0
        filtered_csv_files = 0
        skipped_csv_files = 0
        
        for index, filename in tar_files:
            print(f"  Processing tar file [{processed_files + 1}/{total_files}]:")
            print(f"    File: {filename}")
            print(f"    Index: {index}")
            
            input_file = os.path.join(input_dir, filename)
            
            if output_dir:
                output_file = os.path.join(metric_output_dir, filename)
            else:
                output_file = None
            
            # Process all CSVs in this tar file
            csv_stats = CSVFilter.process_all_csvs_in_tar(input_file, cutoff_timestamp, output_file)
            
            total_csv_files += csv_stats['total']
            filtered_csv_files += csv_stats['filtered']
            skipped_csv_files += csv_stats['skipped']
            processed_files += 1
            print()
        
        print(f"{'='*60}")
        print(f" Summary for {metric_type}:")
        print(f"{'='*60}")
        print(f"  Processed tar files:   {processed_files}")
        print(f"  Total CSV files:       {total_csv_files}")
        print(f"  CSV files with data:   {filtered_csv_files}")
        print(f"  CSV files skipped:     {skipped_csv_files}")
        if output_dir:
            print(f"  Output location:       {metric_output_dir}")
        print()
    
    @staticmethod
    def _get_file_index(filename):
        """Extract index from filename like MSMetrics_0.tar.gz -> 0"""
        match = re.search(r'_(\d+)\.tar\.gz$', filename)
        if match:
            index = int(match.group(1))
            return index
        return -1
    
    @staticmethod
    def _get_sorted_tar_files(input_dir):
        """Get all tar.gz files and sort them by the index"""
        print(f"    Searching for tar.gz files in: {input_dir}")
        tar_files = []
        for filename in os.listdir(input_dir):
            if filename.endswith('.tar.gz'):
                index = CSVFilter._get_file_index(filename)
                tar_files.append((index, filename))
                print(f"      Found: {filename} (index: {index})")
        
        # Sort by index
        tar_files.sort(key=lambda x: x[0])
        sorted_names = [f[1] for f in tar_files]
        print(f"    Sorted order: {sorted_names}")
        return tar_files
    
    @staticmethod
    def process_all_csvs_in_tar(tar_path, cutoff_timestamp, output_tar_path=None):
        """Process all CSVs in a tar.gz file and optionally save to output path"""
        temp_dir = tar_path + '_temp'
        os.makedirs(temp_dir, exist_ok=True)
        print(f"    Extracting to temporary directory:")
        print(f"      Temp dir: {temp_dir}")
        
        stats = {'total': 0, 'filtered': 0, 'skipped': 0}
        
        try:
            # Extract tar.gz file
            with tarfile.open(tar_path, 'r:gz') as tar:
                tar.extractall(path=temp_dir)
                print(f"      SUCCESS: Extracted {os.path.basename(tar_path)}")
            
            filtered_files = []  # Track files that should be included in output
            
            # Process each CSV file in the extracted directory
            print(f"    Searching for CSV files in extracted content:")
            for root, dirs, files in os.walk(temp_dir):
                for file in files:
                    if file.endswith('.csv'):
                        stats['total'] += 1
                        file_path = os.path.join(root, file)
                        print(f"      Found CSV: {file}")
                        
                        if output_tar_path:
                            # Calculate relative output path
                            rel_path = os.path.relpath(file_path, temp_dir)
                            output_csv_path = os.path.join(temp_dir, "filtered", rel_path)
                            
                            # Filter the CSV file
                            max_timestamp_in_file = CSVFilter.filter_csv_copy(file_path, cutoff_timestamp, output_csv_path)
                            
                            # Only include in output if file was actually created
                            if os.path.exists(output_csv_path):
                                filtered_files.append(rel_path)
                                stats['filtered'] += 1
                            else:
                                stats['skipped'] += 1
                        else:
                            # Process in-place
                            temp_output = file_path + '.filtered'
                            max_timestamp_in_file = CSVFilter.filter_csv_copy(file_path, cutoff_timestamp, temp_output)
                            
                            if os.path.exists(temp_output):
                                # Replace original with filtered version
                                os.replace(temp_output, file_path)
                                filtered_files.append(os.path.relpath(file_path, temp_dir))
                                stats['filtered'] += 1
                            else:
                                # Remove the original empty file
                                os.remove(file_path)
                                stats['skipped'] += 1
            
            # Create new tar.gz file only if there are files to include
            if filtered_files:
                if output_tar_path:
                    print(f"    Creating filtered tar.gz file:")
                    print(f"      Output: {output_tar_path}")
                    print(f"      Including {len(filtered_files)} files")
                    
                    with tarfile.open(output_tar_path, 'w:gz') as output_tar:
                        filtered_dir = os.path.join(temp_dir, "filtered")
                        for file_path in filtered_files:
                            full_path = os.path.join(filtered_dir, file_path)
                            output_tar.add(full_path, arcname=file_path)
                    
                    print(f"      SUCCESS: Created filtered tar archive")
                else:
                    # Replace original tar with filtered version (in-place)
                    print(f"    Repacking tar.gz file (in-place):")
                    print(f"      Including {len(filtered_files)} files")
                    
                    new_tar_path = tar_path + '.new'
                    with tarfile.open(new_tar_path, 'w:gz') as new_tar:
                        for file_path in filtered_files:
                            full_path = os.path.join(temp_dir, file_path)
                            new_tar.add(full_path, arcname=file_path)
                    
                    # Replace original tar with filtered version
                    os.replace(new_tar_path, tar_path)
                    print(f"      SUCCESS: Replaced original tar with filtered version")
            else:
                # No files to include - remove tar entirely
                print(f"    No files passed filter - removing tar file")
                if output_tar_path:
                    # Don't create output file
                    print(f"      No output tar created (all files filtered out)")
                else:
                    # Remove original tar file
                    os.remove(tar_path)
                    print(f"      Removed original tar file (all files filtered out)")
            
            print(f"    CSV Processing Summary:")
            print(f"      Total CSV files:    {stats['total']}")
            print(f"      Files with data:    {stats['filtered']}")
            print(f"      Files skipped/empty: {stats['skipped']}")
            
            return stats
            
        except Exception as e:
            print(f"    ERROR: Exception processing {os.path.basename(tar_path)}: {e}")
            import traceback
            traceback.print_exc()
            
            # Don't copy on error - let the file be skipped
            print(f"    ERROR: File will be skipped due to processing error")
            
            return stats
            
        finally:
            # Clean up temporary directory
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
                print(f"    Cleaned up temporary directory")
    
    @staticmethod
    def process_all_metrics(base_dir, cutoff_timestamp, output_dir=None):
        """Process all metric types in the base directory"""
        metric_types = ["MSMetrics", "MSRTMCR", "NodeMetrics"]
        
        for metric_type in metric_types:
            metric_dir = os.path.join(base_dir, metric_type)
            if os.path.exists(metric_dir):
                CSVFilter.process_metric_directory(metric_dir, metric_type, cutoff_timestamp, output_dir)
            else:
                print(f"Warning: Directory {metric_dir} does not exist. Skipping...")

def main():
    parser = argparse.ArgumentParser(
        description="Filter CSV files in tar.gz archives based on timestamp cutoff",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process all metrics and output to contextual directory
  python csv_filter.py path/to/data 239999 --output output/contextual
  
  # Process a specific metric type
  python csv_filter.py path/to/data/MSMetrics 239999 --output output/contextual
  
  # Process all metrics in-place (modify original files)
  python csv_filter.py path/to/data 239999
  
Directory Structure:
  data/                                  output/contextual/
  ├── MSMetrics/                        ├── MSMetrics/
  │   ├── MSMetrics_0.tar.gz     →      │   ├── MSMetrics_0.tar.gz
  │   ├── MSMetrics_1.tar.gz     →      │   ├── MSMetrics_1.tar.gz
  │   └── ...                           │   └── ...
  ├── MSRTMCR/                          ├── MSRTMCR/
  │   ├── MSRTMCR_0.tar.gz       →      │   ├── MSRTMCR_0.tar.gz
  │   └── ...                           │   └── ...
  └── NodeMetrics/                      └── NodeMetrics/
      ├── NodeMetrics_0.tar.gz   →          ├── NodeMetrics_0.tar.gz
      └── ...                               └── ...
        """
    )
    
    parser.add_argument("directory", help="Path to directory containing metric tar.gz files or the base data directory")
    parser.add_argument("cutoff_timestamp", type=int, help="Timestamp cutoff value")
    parser.add_argument("--output", "-o", help="Output directory for filtered data (default: modify files in-place)")
    parser.add_argument("--all", "-a", action="store_true", help="Process all metric types in the base directory")
    parser.add_argument("--type", "-t", help="Specify metric type to process (MSMetrics, MSRTMCR, or NodeMetrics)")
    
    args = parser.parse_args()
    
    if not os.path.exists(args.directory):
        print(f"Error: Directory {args.directory} does not exist")
        sys.exit(1)
    
    print("\n" + "="*60)
    print("CSV FILTER TOOL")
    print("="*60)
    print(f"Directory: {args.directory}")
    print(f"Cutoff Timestamp: {args.cutoff_timestamp}")
    if args.output:
        print(f"Output Directory: {args.output}")
    else:
        print("Mode: In-place modification (no output directory specified)")
    print(f"Started at: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    if args.all or (not args.type and os.path.basename(args.directory) not in ["MSMetrics", "MSRTMCR", "NodeMetrics"]):
        # Process all metrics in the directory
        CSVFilter.process_all_metrics(args.directory, args.cutoff_timestamp, args.output)
    else:
        # Process a specific metric type
        if args.type:
            metric_dir = os.path.join(args.directory, args.type)
            metric_type = args.type
        else:
            metric_dir = args.directory
            metric_type = os.path.basename(args.directory)
        
        CSVFilter.process_metric_directory(metric_dir, metric_type, args.cutoff_timestamp, args.output)
    
    print("="*60)
    print(f"Completed at: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)

if __name__ == "__main__":
    main()

    # python3 csv_filter.py clusterdata/cluster-trace-microservices-v2022/data 239999 --output output/contextual