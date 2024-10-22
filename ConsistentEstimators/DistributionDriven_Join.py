#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Aug  8 16:41:09 2024

Contains classes to handle MCAR  and MAR joins 
- one relation 
- relation joins (ripple join is implemented )
- after joining, it proceeds with algorithms for one relation

"""

import pandas as pd
import aggregateFunctions
import FullJointDistEstimation

class DistributionDrivenJoin:
    def __init__(self, relation1File, relation2File, joinCol1, joinCol2, predicate, attr, conditions=[], TypeOfmissingness='mcar', originalN=1):
        self.csv_file1 = relation1File
        self.csv_file2 = relation2File
        self.column_r = joinCol1
        self.column_s = joinCol2
        self.operator = predicate
       # self.attribute= attr # attribute to aggregate
        #self.aggregation_func = aggFunc
        self.attribute = attr[1]
        self.aggregation_func = attr[0]
        self.TypeOfmissingness= TypeOfmissingness
        self.conditions=conditions
        self.originalN=originalN
        #self.CauseMAR_att=CauseMAR_att
        

        
    def read_csv(self, file_path):
        
        return pd.read_csv(file_path)

    def predicate(self, row_r, row_s):
        
        if self.operator == '==':
            return row_r[self.column_r] == row_s[self.column_s]
        elif self.operator == '>':
            return row_r[self.column_r] > row_s[self.column_s]
        elif self.operator == '<':
            return row_r[self.column_r] < row_s[self.column_s]
        else:
            raise ValueError("Unsupported operator")

    def ripple_join(self): 
      
  
        if self.TypeOfmissingness.lower()=='mcar':

           estim_joint_r = FullJointDistEstimation.obtainJointDist(self.csv_file1)
           distribution_r = self.read_csv(estim_joint_r.getMCARFullJointDist(self.attribute))
           estim_joint_s = FullJointDistEstimation.obtainJointDist(self.csv_file2)
           distribution_s = self.read_csv(estim_joint_s.getMCARFullJointDist(self.attribute))
           
        if self.TypeOfmissingness.lower()=='mar':
            estim_joint_r = FullJointDistEstimation.obtainJointDist(self.csv_file1)
            distribution_r = self.read_csv(estim_joint_r.getMARFullJointDist(self.column_r))
            estim_joint_s = FullJointDistEstimation.obtainJointDist(self.csv_file2)
            distribution_s = self.read_csv(estim_joint_s.getMARFullJointDist(self.column_s))

# =============================================================================
#          # for debug   
#         distribution_r=self.read_csv(self.csv_file1)
#         distribution_s=self.read_csv(self.csv_file2)
# =============================================================================
# =============================================================================
#         print(distribution_r['P'].sum())
#         print()
#         print(distribution_r.head(5))
#         r_subset=distribution_r.head(5)
#         r_subset.to_csv('r_head_d.csv', index=False)
#         print()
#         print(distribution_s.head(5))
#         s_subset=distribution_s.head(5)
#         s_subset.to_csv('s_head_d.csv', index=False)
# =============================================================================
        max_value = max(len(distribution_r), len(distribution_s))
        
        # Initialize an empty DataFrame to store the result ## U 
        result = pd.DataFrame()
        
        for max_idx in range(1, max_value + 1):
            for i in range(1, max_idx):
                if i < len(distribution_r) and max_idx-1 < len(distribution_s):
                    row_r = distribution_r.iloc[i-1]
                    row_s = distribution_s.iloc[max_idx-1]
                    if self.predicate(row_r, row_s):
                        combined_row = pd.concat([row_r, row_s], axis=0).to_frame().T  # Combine rows as a DataFrame
                        combined_row['P'] = row_r['P'] * row_s['P']  # Calculate joint probability
                        result = pd.concat([result, combined_row], ignore_index=True)
            
            for i in range(1, max_idx + 1):
                if max_idx-1 < len(distribution_r) and i-1 < len(distribution_s):
                    row_r = distribution_r.iloc[max_idx-1]
                    row_s = distribution_s.iloc[i-1]
                    if self.predicate(row_r, row_s):
                        combined_row = pd.concat([row_r, row_s], axis=0).to_frame().T  # Combine rows as a DataFrame
                        combined_row['P'] = row_r['P'] * row_s['P']  # Calculate joint probability
                        result = pd.concat([result, combined_row], ignore_index=True)
        
        # Remove duplicate columns (if any) that result from the concatenation
        result = result.loc[:, ~result.columns.duplicated()]
         
        # Check if the column exists before trying to drop it
        if self.column_s + '_y' in result.columns:
            result = result.drop(columns=[self.column_s + '_y'])  # Drop the duplicate join column if exists
        
        if self.column_r + '_x' in result.columns or self.column_s + '_x' in result.columns:
            result.rename(columns={self.column_r + '_x': self.column_r, self.column_s + '_x': self.column_s}, inplace=True)
        
        # Normalize the P column so that the sum equals 1
        total_p = result['P'].sum()
        result['P'] = result['P'] / total_p
        # Move the P column to the last position
        cols = [col for col in result.columns if col != 'P'] + ['P']
        result = result[cols]
       # print(result)
       # print(result['P'].sum())
        #return result
        return self.callForAgg(result)
     
    def callForAgg(self, joinResult):
        #jpd_filename='ripple_join_Dist2_result.csv'
        jpd_filename= self.csv_file1.replace('.csv', '_jointDistU_table.csv')
        joinResult.to_csv(jpd_filename, index=False)
        #if self.TypeOfmissingness.lower()=='mcar':
        test =aggregateFunctions.aggregateFunctions(self.originalN)
        answer=test.mcarAVG(jpd_filename,  [self.aggregation_func,self.attribute],self.conditions)
        return answer
            
            


