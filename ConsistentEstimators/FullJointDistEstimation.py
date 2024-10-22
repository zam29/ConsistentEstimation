#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Mar 31 20:00:49 2024

given an observed distribution table, compute the full joint probability distribution P(AT)
"""

import pandas as pd
import Multi_UI
import os
import time 

class obtainJointDist:
    """class recives joint probabltiy table as csv file and computes the joint disirbution of MCAR and MAR"""
    """joint probablity tables are created in sampling.py"""
    def __init__(self, csv_filename):
        self.csv_filename=csv_filename
    def read_csv(self, file_path):
            return pd.read_csv(file_path)
    def getMARFullJointDist(self,attr):
        df = self.read_csv(self.csv_filename)

        q= "Select From"
        #new_q = f"Select {columns_str} from {self.csv_filename}"
        new_q = f"Select {attr} from {self.csv_filename}"
        if attr in df.columns: # the attr alwasy have mising vals so she always have a cause 
            columns_list = df.columns.tolist()
            columns_str = ', '.join( columns_list)
            columns_with_missing = df.columns[df.isnull().any()].tolist()
            if columns_with_missing:
                columns_str = ', '.join( columns_with_missing)
           # new_q = f"Select {attr} from {self.csv_filename}"
            start_time = time.time()
            new_q = f"Select {columns_str} from {self.csv_filename}"
            UITest =Multi_UI.UI(isDrisbutionDriven_forFullMARJoin=True,GetMARFullJointDist=True)
            UITest.run([new_q, "mar"])
            end_time = time.time()
           # print("cobstructed mar full dist table time ", end_time - start_time)
# =============================================================================
#             with open('ModelBasedLog/ModelBased_timing_log.txt', 'a') as f:
#                 f.write(f"in Function 'get mar full joint dist' executed in {end_time - start_time:.4f} seconds. file = {self.csv_filename}\n")           
# =============================================================================
        directory, filename_with_ext = os.path.split(self.csv_filename)
        filename, ext = os.path.splitext(filename_with_ext)
        #new_filename = filename + '_jpt' + ext
        # Modify the filename by adding "new_file_" at the beginning
        new_filename = filename +  ext
        new_filename = "MAR_FullJoint_" + new_filename
        
        # Create the output file path
        output_csv = os.path.join(directory, new_filename)
        return output_csv
        #return 'MAR_FullJoint_'+self.csv_filename
        #print("Done executing")
       # pass
    def getMCARFullJointDist(self,attribute):
        #output_csv='MCAR_FullJoint_'+self.csv_filename
        directory, filename = os.path.split(self.csv_filename)
        output_csv = os.path.join(directory, f"MCAR_FullJoint_{filename}")
        df = self.read_csv(self.csv_filename)
        # Drop rows with NaN values in any of the columns and create a copy to avoid SettingWithCopyWarning
        if attribute in df.columns:
            df_clean = df[df[attribute].notna()]
        else:
            df_clean = df.dropna().copy() # since its MCAR 
      
        # Extract column names 
        cols = df_clean.columns
        if len(cols) < 2:
            raise ValueError("Input file must contain at least one variable column and one probability column.")
        start_time = time.time()
        # the last column is the probability column
        prob_col = cols[-1]
        variable_cols = cols[:-1]

        # Compute the sum of probabilities where there are no NaN values;  P(RZ=0)
        sum_p = df_clean[prob_col].sum() #  P(RZ=0)
        # Compute the new probability as P(A,B) / sum_p using .loc
        df_clean.loc[:, 'P'] = df_clean[prob_col] / sum_p

        # Keep only the relevant columns: A, B, and the normalized probability
        result_df = df_clean[variable_cols.tolist() + ['P']]
        end_time = time.time()
        print("cobstructed mcar full dist table time ", end_time - start_time)
# =============================================================================
#         with open('ModelBasedLog/ModelBased_timing_log.txt', 'a') as f:
#                 f.write(f"in Function 'get mcar full joint dist' executed in {end_time - start_time:.4f} seconds. file = {self.csv_filename}\n")
# =============================================================================
        #print("Resulting DataFrame with normalized probabilities:")
        #print(result_df)
        #Total = result_df['P'].sum()
        #print(Total) #should be = 1
        # Save the result to a new CSV file
        result_df.to_csv(output_csv, index=False)
        return output_csv
        
    
    