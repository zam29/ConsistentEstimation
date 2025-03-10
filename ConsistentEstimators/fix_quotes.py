#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""


a helper file for real world data, as they may have cells with unterminated quotes 
"""

import pandas as pd

# Function to detect and fix unterminated quotes in any column of the DataFrame
def fix_unterminated_quotes(df):
    # Iterate over all columns in the DataFrame
    for column in df.columns:
        if df[column].dtype == 'object':  # Only apply to object (string) columns
            # Detect rows with unterminated or unmatched single quotes
            unterminated_rows = df[column].str.contains(r"'", regex=True, na=False)
            
            # If there are any unterminated quotes in this column, fix them
            if unterminated_rows.any():
                # Option 1: Remove all single quotes
                df[column] = df[column].str.replace("'", "", regex=False)
                
                # Option 2 (alternative): Attempt to fix unterminated quotes (if needed)
                # df[column] = df[column].str.replace(r"(?<!')'(?!')", "'", regex=True)
                
                print(f"Fixed unterminated quotes in column: {column}")
    
    return df

# Read the CSV file into a DataFrame
csv_file_path = 'rwDatasets/Employee_mar.csv'  # Replace with your CSV file path
df = pd.read_csv(csv_file_path)

# Apply the function to fix unterminated quotes in all columns
df = fix_unterminated_quotes(df)

# Save the cleaned DataFrame to a new CSV file (optional)
df.to_csv('rwDatasets/Employee_mar.csv', index=False)

# Display the cleaned DataFrame
print(df.head())
