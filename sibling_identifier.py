import pandas as pd
import os
from collections import defaultdict
import csv

# Try to import CSVFilter, but continue if it's not available
try:
    from csv_filter import CSVFilter
    CSV_FILTER_AVAILABLE = True
except ImportError:
    print("Warning: csv_filter module not found. Contextual data processing will be skipped.")
    CSV_FILTER_AVAILABLE = False
    CSVFilter = None

class SimpleSiblingAnalyzer:
    def __init__(self, input_folder):
        """Initialize with the input folder containing MSCallGraph files"""
        self.input_folder = input_folder
        self.largest_timestamp = None
        self.processed_files = []
        self.file_stats = {}
        self.output_dir = None
        self.sibling_file_handles = {}  # Store open file handles
        self.sibling_writers = {}       # Store CSV writers
        
        print("\n" + "="*60)
        print(f"DIRECT-WRITE SIBLING PAIR ANALYZER")
        print("="*60)
        print(f"Input folder: {input_folder}")
        print("-"*60)
    
    def setup_output_structure(self, output_dir):
        """Set up output directory structure"""
        self.output_dir = output_dir
        os.makedirs(os.path.join(output_dir, "siblings"), exist_ok=True)
    
    def parse_rpcid(self, rpcid):
        """Parse rpcid to get parent prefix and last segment"""
        parts = rpcid.split('.')
        if len(parts) <= 1:
            return rpcid, ""
        parent_prefix = '.'.join(parts[:-1])
        last_segment = parts[-1]
        return parent_prefix, last_segment
    
    def analyze_execution_order(self, s1, s2):
        """Determine execution order between two siblings"""
        s1_start = s1['timestamp']
        s1_end = s1['timestamp'] + s1['rt']
        s2_start = s2['timestamp']
        s2_end = s2['timestamp'] + s2['rt']
        
        if s1_end <= s2_start:
            return 'sequential'
        elif s2_end <= s1_start:
            return 'sequential'
        else:
            return 'concurrent'
    
    def get_sibling_filename(self, dm1, dm2):
        """Get standard filename for a sibling pair"""
        sorted_names = tuple(sorted([dm1, dm2]))
        filename = f"sibling_{sorted_names[0]}_{sorted_names[1]}.csv"
        return os.path.join(self.output_dir, "siblings", filename)
    
    def get_writer(self, dm1, dm2, create_new=False):
        """Get or create a CSV writer for the sibling pair"""
        key = tuple(sorted([dm1, dm2]))
        filename = self.get_sibling_filename(dm1, dm2)
        
        if key not in self.sibling_writers or create_new:
            file_exists = os.path.exists(filename)
            
            # Open file in append mode
            file_handle = open(filename, 'a', newline='')
            self.sibling_file_handles[key] = file_handle
            
            # Create CSV writer
            writer = csv.DictWriter(file_handle, fieldnames=[
                'traceid', 'rpcid', 'um', 'uminstanceid',
                'dm1', 'dminstanceid1', 'dm1_start_time',
                'dm2', 'dminstanceid2', 'dm2_start_time',
                'execution_order'
            ])
            
            # Write header if file is new
            if not file_exists:
                writer.writeheader()
            
            self.sibling_writers[key] = writer
        
        return self.sibling_writers[key]
    
    def write_record(self, record):
        """Write a single record directly to the appropriate CSV file with consistent dm1/dm2 ordering"""
        # Create a copy of the record to avoid modifying the original
        ordered_record = record.copy()
        
        # Ensure consistent ordering: always put the lexicographically smaller service as dm1
        if record['dm1'] > record['dm2']:
            # Swap the dm1 and dm2 data
            ordered_record['dm1'] = record['dm2']
            ordered_record['dm2'] = record['dm1']
            ordered_record['dminstanceid1'] = record['dminstanceid2']
            ordered_record['dminstanceid2'] = record['dminstanceid1']
            ordered_record['dm1_start_time'] = record['dm2_start_time']
            ordered_record['dm2_start_time'] = record['dm1_start_time']
        
        # Get writer with the consistently ordered pair
        writer = self.get_writer(ordered_record['dm1'], ordered_record['dm2'])
        writer.writerow(ordered_record)

    # Additionally, you should update create_record logic to ensure consistent creation:
    def create_record(self, traceid, prefix, um, s1, s2, execution_order):
        """Create a record with consistent dm1/dm2 ordering"""
        # Always put the lexicographically smaller service as dm1
        if s1['dm'] > s2['dm']:
            dm1_data = s2
            dm2_data = s1
        else:
            dm1_data = s1
            dm2_data = s2
        
        return {
            'traceid': traceid,
            'rpcid': prefix,
            'um': um,
            'uminstanceid': dm1_data['uminstanceid'],
            'dm1': dm1_data['dm'],
            'dminstanceid1': dm1_data['dminstanceid'],
            'dm1_start_time': dm1_data['timestamp'],
            'dm2': dm2_data['dm'],
            'dminstanceid2': dm2_data['dminstanceid'],
            'dm2_start_time': dm2_data['timestamp'],
            'execution_order': execution_order
        }

    # Modified process_single_file method to use the new create_record:
    def process_single_file(self, df, file_idx, total_files):
        """Process a single CSV file's data to find siblings"""
        sibling_stats = defaultdict(lambda: {'total': 0, 'parallel': 0, 'sequential': 0})
        records_written = 0
        
        # Group by traceid and um
        grouped = df.groupby(['traceid', 'um'])
        
        for (traceid, um), group in grouped:
            # Parse rpcids and group by parent prefix
            prefix_groups = defaultdict(list)
            
            for idx, row in group.iterrows():
                parent_prefix, last_segment = self.parse_rpcid(row['rpcid'])
                prefix_groups[parent_prefix].append({
                    'dm': row['dm'],
                    'dminstanceid': row['dminstanceid'],
                    'uminstanceid': row['uminstanceid'],
                    'timestamp': row['timestamp'],
                    'rt': row['rt'],
                    'rpcid': row['rpcid'],
                    'service': row['service'],
                    'interface': row['interface']
                })
            
            # For each prefix group, find sibling pairs
            for prefix, siblings in prefix_groups.items():
                if len(siblings) >= 2:
                    # Create all sibling pairs
                    for i in range(len(siblings)):
                        for j in range(i + 1, len(siblings)):
                            s1, s2 = siblings[i], siblings[j]
                            if s1['dm'] != s2['dm']:  # Different downstream services
                                execution_order = self.analyze_execution_order(s1, s2)
                                
                                # Create record with consistent ordering
                                record = self.create_record(traceid, prefix, um, s1, s2, execution_order)
                                
                                # Write directly to file
                                self.write_record(record)
                                records_written += 1
                                
                                # Track statistics with consistent key
                                key = tuple(sorted([s1['dm'], s2['dm']]))
                                sibling_stats[key]['total'] += 1
                                if execution_order == 'concurrent':
                                    sibling_stats[key]['parallel'] += 1
                                else:
                                    sibling_stats[key]['sequential'] += 1
        
        # Print statistics for this file
        print(f"   ✓ Processed {records_written:,} sibling records")
        print(f"   ✓ Found {len(sibling_stats):,} unique sibling pairs")

        # Show top 5 most frequent sibling pairs
        if sibling_stats:
            sorted_stats = sorted(sibling_stats.items(), key=lambda x: x[1]['total'], reverse=True)
            print(f"   ✓ Top 5 sibling pairs:")
            for (dm1, dm2), stats in sorted_stats[:5]:
                print(f"      • {dm1}-{dm2}: {stats['total']:,} total")
    
    def cleanup(self):
        """Close all file handles"""
        for file_handle in self.sibling_file_handles.values():
            file_handle.close()
    
    def analyze_output_files(self):
        """Analyze the final output files"""
        sibling_dir = os.path.join(self.output_dir, "siblings")
        sibling_files = [f for f in os.listdir(sibling_dir) if f.endswith('.csv')]
        
        print("\n" + "="*60)
        print("OUTPUT FILES ANALYSIS")
        print("="*60)
        
        total_records = 0
        total_parallel = 0
        total_sequential = 0
        
        for file in sibling_files:
            file_path = os.path.join(sibling_dir, file)
            df = pd.read_csv(file_path)
            
            unique_traces = df['traceid'].nunique()
            execution_counts = df['execution_order'].value_counts()
            
            file_parallel = execution_counts.get('concurrent', 0)
            file_sequential = execution_counts.get('sequential', 0)
            
            total_records += len(df)
            total_parallel += file_parallel
            total_sequential += file_sequential

        
        print("\n" + "-"*60)
        print(f"TOTAL SUMMARY:")
        print(f"   • Unique sibling pairs: {len(sibling_files):,}")
        print(f"   • Total records: {total_records:,}")
        print(f"   • Parallel executions: {total_parallel:,} ({total_parallel/total_records*100:.1f}%)")
        print(f"   • Sequential executions: {total_sequential:,} ({total_sequential/total_records*100:.1f}%)")
        print("-"*60)
    
    def run_analysis(self, output_dir="output"):
        """Run the complete analysis pipeline"""
        print("\n⚡ STARTING DIRECT-WRITE SIBLING ANALYSIS")
        print("="*60)
        
        # Set up output structure
        self.setup_output_structure(output_dir)
        
        # Get all CSV files
        csv_files = [f for f in os.listdir(self.input_folder) if f.endswith('.csv')]
        
        if not csv_files:
            raise ValueError(f"No CSV files found in {self.input_folder}")
        
        print(f"\nFound {len(csv_files)} CSV files to process")
        print("-"*60)
        
        try:
            # Process each file
            for idx, csv_file in enumerate(csv_files, 1):
                print(f"\n[{idx}/{len(csv_files)}] PROCESSING FILE: {csv_file}")
                print("-" * 40)
                
                file_path = os.path.join(self.input_folder, csv_file)
                
                # Load file
                try:
                    df = pd.read_csv(file_path)
                except pd.errors.ParserError:
                    print(f"⚠️  Error reading with default settings. Trying error handling...")
                    try:
                        df = pd.read_csv(file_path, on_bad_lines='skip')
                    except TypeError:
                        df = pd.read_csv(file_path, engine='python', error_bad_lines=False)
                
                print(f"   ✓ Successfully loaded: {len(df):,} rows")
                
                # Update timestamp
                file_max_timestamp = df['timestamp'].max()
                if self.largest_timestamp is None or file_max_timestamp > self.largest_timestamp:
                    self.largest_timestamp = file_max_timestamp
                
                # Process this file
                self.process_single_file(df, idx, len(csv_files))
                self.processed_files.append(csv_file)
        
        finally:
            # Always close file handles
            self.cleanup()
        
        # Analyze output files
        self.analyze_output_files()
        
        # Final summary
        print("\n" + "="*60)
        print("🎉 ANALYSIS COMPLETE!")
        print("="*60)
        print(f"\n📁 OUTPUT LOCATION: {output_dir}/siblings/")
        print(f"⏱️ Largest timestamp: {self.largest_timestamp}")
        print("\n" + "="*60 + "\n")

# Example usage
if __name__ == "__main__":
    # Initialize simple analyzer
    analyzer = SimpleSiblingAnalyzer("capser-output-2022/output-rebuild")
    
    # Run analysis without processing contextual data
    analyzer.run_analysis(output_dir="output")
    