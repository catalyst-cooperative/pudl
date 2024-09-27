import pandas as pd
import glob
import os

# Path to your CSV files, assuming they are all in the same directory
csv_files = glob.glob('/Users/sam/Documents/pudl/src/pudl/package_data/phmsagas/column_maps/*.csv')

for file in csv_files:
    # Get the base name of the file (excluding the path) to compare with 'page_part.csv'
    if os.path.basename(file) == 'page_part_map.csv':
        print(f"Skipping {file}.")
        continue  # Skip this file
    
    # Load the CSV file into a dataframe
    df = pd.read_csv(file)
    
    # Ensure the last column in the header is for 2022, before adding 2023
    if df.columns[-1] == '2022':
        # Create a new "2023" column by copying the "2022" column
        df['2023'] = df['2022']
        
        # Replace occurrences of '2022' with '2023' and '22' with '23' in the new "2023" column
        df['2023'] = df['2023'].astype(str).replace('2022', '2023', regex=True)
        df['2023'] = df['2023'].astype(str).replace(r'\b22\b', '23', regex=True)

        # Save the modified dataframe back to the CSV file (overwrite)
        df.to_csv(file, index=False)

        print(f"Updated {file} successfully.")
    else:
        print(f"Skipped {file}, as the last column is not 2022.")
