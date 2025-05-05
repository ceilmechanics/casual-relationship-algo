#!/usr/bin/env python3
"""
Find the 5 largest CSV files in output/res/uncertain directory that:
1. Don't have "(?" or "unknown" in their column values
2. Have both concurrent and sequential execution orders
3. Show both top 5 overall and top 5 with mixed execution patterns
"""

import pandas as pd
import os
import glob
import re

def contains_unwanted_values(df):
    """Check if dataframe contains '(?' or 'unknown' in any column values"""
    for column in df.columns:
        for value in df[column].astype(str):
            if "(?" in value or "unknown" in value.lower():
                return True
    return False

def has_mixed_execution_orders(df):
    """Check if dataframe has both concurrent and sequential execution orders"""
    if 'execution_order' not in df.columns:
        return False
    
    execution_orders = df['execution_order'].unique()
    has_concurrent = 'concurrent' in execution_orders
    has_sequential = 'sequential' in execution_orders
    
    return has_concurrent and has_sequential

def find_largest_csvs():
    """Find the largest CSV files both overall and with mixed execution patterns"""
    
    # Get all CSV files in uncertain directory
    uncertain_path = "output/res/uncertain"
    if not os.path.exists(uncertain_path):
        print(f"Error: Directory {uncertain_path} does not exist")
        return
    
    csv_files = glob.glob(os.path.join(uncertain_path, "*.csv"))
    
    print(f"Found {len(csv_files)} CSV files in {uncertain_path}")
    print("-" * 50)
    
    # Store file info: (filename, row_count, has_unwanted_values, has_mixed_orders)
    file_info = []
    
    for csv_file in csv_files:
        try:
            df = pd.read_csv(csv_file)
            row_count = len(df)
            has_unwanted = contains_unwanted_values(df)
            has_mixed = has_mixed_execution_orders(df)
            
            # Count execution order types
            if 'execution_order' in df.columns:
                concurrent_count = sum(df['execution_order'] == 'concurrent')
                sequential_count = sum(df['execution_order'] == 'sequential')
                other_count = len(df) - concurrent_count - sequential_count
                execution_summary = f"Concurrent: {concurrent_count}, Sequential: {sequential_count}"
                if other_count > 0:
                    execution_summary += f", Other: {other_count}"
            else:
                execution_summary = "No execution_order column"
            
            file_info.append({
                'filename': os.path.basename(csv_file),
                'path': csv_file,
                'row_count': row_count,
                'has_unwanted': has_unwanted,
                'has_mixed_orders': has_mixed,
                'execution_summary': execution_summary
            })
            
            # Print status
            unwanted_status = "❌ Contains (?) or unknown" if has_unwanted else "✅ Clean"
            mixed_status = "✅ Mixed execution" if has_mixed else "❌ Single execution type"
            print(f"{os.path.basename(csv_file)}:")
            print(f"  Rows: {row_count}")
            print(f"  Values: {unwanted_status}")
            print(f"  Execution: {mixed_status}")
            print(f"  {execution_summary}")
            print("-" * 30)
            
        except Exception as e:
            print(f"Error reading {csv_file}: {e}")
    
    # Filter for clean files only
    clean_files = [f for f in file_info if not f['has_unwanted']]
    
    # Filter for files that are clean AND have mixed execution orders
    mixed_files = [f for f in clean_files if f['has_mixed_orders']]
    
    # Sort by row count in descending order
    clean_files.sort(key=lambda x: x['row_count'], reverse=True)
    mixed_files.sort(key=lambda x: x['row_count'], reverse=True)
    
    # Get top 5 for each category
    top_5_overall = clean_files[:5]
    top_5_mixed = mixed_files[:5]
    
    print("\n" + "="*70)
    print("🏆 TOP 5 LARGEST CSV FILES (BY TOTAL OBSERVATIONS)")
    print("WITHOUT (?) or unknown values:")
    print("="*70)
    
    for i, file in enumerate(top_5_overall, 1):
        print(f"{i}. {file['filename']}")
        print(f"   Rows: {file['row_count']}")
        print(f"   Execution: {file['execution_summary']}")
        print(f"   Path: {file['path']}")
        print("-" * 70)
    
    print("\n" + "="*70)
    print("🔄 TOP 5 LARGEST CSV FILES WITH MIXED EXECUTION PATTERNS")
    print("(Concurrent + Sequential) AND WITHOUT (?) or unknown values:")
    print("="*70)
    
    for i, file in enumerate(top_5_mixed, 1):
        print(f"{i}. {file['filename']}")
        print(f"   Rows: {file['row_count']}")
        print(f"   Execution: {file['execution_summary']}")
        print(f"   Path: {file['path']}")
        print("-" * 70)
    
    # Summary statistics
    total_files = len(file_info)
    clean_files_count = len(clean_files)
    mixed_execution_count = len(mixed_files)
    unwanted_files_count = total_files - clean_files_count
    
    print("\n" + "="*50)
    print("📊 SUMMARY STATISTICS:")
    print("="*50)
    print(f"Total CSV files found: {total_files}")
    print(f"Files without (?) or unknown: {clean_files_count}")
    print(f"Files with mixed execution orders: {mixed_execution_count}")
    print(f"Files with (?) or unknown: {unwanted_files_count}")
    
    if clean_files:
        print(f"\nLargest overall file: {clean_files[0]['filename']} ({clean_files[0]['row_count']} rows)")
        if mixed_files:
            print(f"Largest mixed execution file: {mixed_files[0]['filename']} ({mixed_files[0]['row_count']} rows)")
        else:
            print("No files with mixed execution patterns found")
    print("="*50)
    
    return top_5_overall, top_5_mixed

if __name__ == "__main__":
    find_largest_csvs()