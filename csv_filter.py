import pandas as pd
import os

def preprocess_msrtmcr(input_folder='output/data/MSRTMCR', output_folder='output/data/MSRTMCR_cleaned'):
    # Columns to retain
    keep_columns = ['timestamp', 'msname', 'msinstanceid', 'nodeid', 'providerrpc_mcr', 'consumerrpc_mcr']

    # Create output directory if it doesn't exist
    os.makedirs(output_folder, exist_ok=True)

    # Process each CSV file
    for filename in os.listdir(input_folder):
        if filename.endswith('.csv'):
            file_path = os.path.join(input_folder, filename)
            df = pd.read_csv(file_path)

            # Keep only desired columns
            cleaned_df = df[[col for col in keep_columns if col in df.columns]]

            # Save to new file
            output_path = os.path.join(output_folder, filename)
            cleaned_df.to_csv(output_path, index=False)
            print(f"Processed: {filename}")

if __name__ == "__main__":
    preprocess_msrtmcr()