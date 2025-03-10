#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct  2 12:35:35 2023
computes the observed data distribution PT
"""

import pandas as pd
import csv

class JointProbability :
    def __init__(self):
        pass
    def compute_joint_probability(self, input_csv):
        """
        Compute the joint probability table for each row in the input CSV file and add it as a new column in the output CSV file.
    
        Args:
            input_csv (str): Path to the input CSV file.
            output_csv (str): Path to the output CSV file.
    
        Returns:
            None
        """
        output_csv = "data_processing/"+input_csv.rsplit(".", 1)[0].split("/")[-1]+"_jpt.csv"
        df = pd.read_csv(input_csv)
        N = len(df)
    
        # Fill NaN values with a unique placeholder to include them in the counting
        placeholder = 'NaN'
        df_filled = df.where(pd.notnull(df), placeholder)
        
        count_column_name = 'count_tmpabc'
        # Group by all columns to get counts
        grouped = df_filled.groupby(list(df_filled.columns)).size().reset_index(name=count_column_name)
        
        # Calculate joint probability
        grouped['P'] = grouped[count_column_name] / N
        
        # Replace placeholder with NaN
        grouped.replace(placeholder, float('nan'), inplace=True)
        
        # Drop the temporary count column and save the result to CSV
        grouped.drop(columns=[count_column_name], inplace=True)
        grouped.to_csv(output_csv, index=False, na_rep='NaN')
        return output_csv,N
