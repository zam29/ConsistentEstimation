#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Sep 19 11:52:35 2024

Law of total expectation and law of total variance for sample-based MAR
it reads row by row to keep track of each group P(X=x) distribution

ToDo: instead of per row update make the update in batches for faster execution

"""

import pandas as pd
import numpy as np
import math
from scipy.stats import norm
import os
import time 
import csv

output_dir = "Mar_CI_results"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

class TotalVarianceCI:
    def __init__(self, confidence_level=0.95,condition_attr=None, conditionOperator=None, valueOfZcondition=None):
        self.z_score = norm.ppf((1 + confidence_level) / 2)  # Precompute Z-score
        self.group_stats = {}  # Store statistics per group
        self.Z_count = 0  # Counter when the condition is met
        self.X_counts = {}  # Store counts for each X
        self.total_count = 0  # Store total number of rows
        self.condition_attr= condition_attr
        self.conditionOperator= conditionOperator
        self.valueOfZcondition = valueOfZcondition
        
    def calculate_min_sample_size(self, sigma, margin_of_error, big_N):
        """Calculate the minimum sample size dynamically based on the estimated standard deviation (sigma)."""
        min_sample_size = (self.z_score * sigma / margin_of_error) ** 2
        return min(int(np.ceil(min_sample_size)), big_N)

    def compare_as_numbers(self, Z, Z_condition):
        flag = True
        for i in range (0, len(self.conditionOperator)):
            if self.conditionOperator[i] == '==':
                try:
                    # Attempt to convert both values to floats
                    if not float(Z) == float(Z_condition):
                        flag =False
                except ValueError:
                    # If conversion fails, fall back to string comparison
                    if not str(Z) == str(Z_condition):
                        flag =False
            elif self.conditionOperator[i] == '>':
                try:
                    # Attempt to convert both values to floats
                    if not float(Z) > float(Z_condition):
                        flag =False
                except ValueError:
                    # If conversion fails, fall back to string comparison
                    if not str(Z) > str(Z_condition):
                        flag =False
            elif self.conditionOperator[i] == '<':
                try:
                    # Attempt to convert both values to floats
                    if not float(Z) < float(Z_condition):
                        flag =False
                except ValueError:
                    # If conversion fails, fall back to string comparison
                    if not str(Z) < str(Z_condition):
                        flag =False
        return flag
        

    def update(self, Y, A_o, Z=None, Z_condition=None, misc_flag=False, flag=True):
        """Incrementally update the group statistics with a new sample, considering Z_condition."""
        group_key = (A_o, Z)  # Now include Z as part of the group key
        
        # Increment total count for P(X)
        self.total_count += 1
        if A_o not in self.X_counts:
            self.X_counts[A_o] = 0
        self.X_counts[A_o] += 1
        if Z_condition is not None:
            # Only update if the condition on Z is met
            if self.compare_as_numbers(Z, Z_condition):
                self.Z_count += 1
               # print(f"Processing row with group: {group_key}")
                if flag:
                    if group_key not in self.group_stats:
                        self.group_stats[group_key] = {"nx": 0, "r": 0, "sum_Y": 0.0, "sum_squared_Y": 0.0, "P": 0.0, "Y_hat": 0.0}
                if misc_flag:
                    self.group_stats[group_key]["r"] += 1
                if Y is not None:
                    self.group_stats[group_key]["nx"] += 1
                    self.group_stats[group_key]["sum_Y"] += Y
                    self.group_stats[group_key]["sum_squared_Y"] += Y ** 2
                    self.group_stats[group_key]["Y_hat"] = self.group_stats[group_key]["sum_Y"] / self.group_stats[group_key]["nx"]
        if Z_condition is None: # there is no condtion 
            if flag:
                if group_key not in self.group_stats:
                    self.group_stats[group_key] = {"nx": 0, "r": 0, "sum_Y": 0.0, "sum_squared_Y": 0.0, "P": 0.0, "Y_hat": 0.0}
            if misc_flag:
                self.group_stats[group_key]["r"] += 1
            if Y is not None:
                self.group_stats[group_key]["nx"] += 1
                self.group_stats[group_key]["sum_Y"] += Y
                self.group_stats[group_key]["sum_squared_Y"] += Y ** 2
                self.group_stats[group_key]["Y_hat"] = self.group_stats[group_key]["sum_Y"] / self.group_stats[group_key]["nx"]

    def compute_P_X_given_Z(self,n):
        """Compute P(X | Z) using the formula: P(X|Z) = P(Z|X)P(X) / sum_x P(Z|X)P(X)"""
        # First calculate P(Z | X) and P(X)
        if self.Z_count  !=0:
            PXZ = {}
            for group_key in self.group_stats:
                A_o, Z = group_key[0], group_key[1]
                P_Z_given_X = self.group_stats[group_key]["r"] / self.X_counts[A_o]  # P(Z | X)
                P_X = self.X_counts[A_o] / self.total_count  # P(X)
                PXZ[group_key] = P_Z_given_X * P_X
    
            # Normalize P(X | Z)
            normalization_factor = sum(PXZ.values())
            for group_key in PXZ:
                self.group_stats[group_key]["P"] = PXZ[group_key] / normalization_factor
        else: 
            for w in self.group_stats:
                self.group_stats[w]["P"] = self.group_stats[w]["r"] / (n ) 

    def getCurrentExpectation(self, n):
        # Compute P(X | Z)
        self.compute_P_X_given_Z(n)

        # Compute the final weighted expectation
        current_exp = sum(self.group_stats[w]["Y_hat"] * self.group_stats[w]["P"] for w in self.group_stats)
        print(f" current Exp: {current_exp}")
        print("==================================================")

    def compute_confidence_interval(self, n):
        """Compute the variance and confidence interval using Law of Total Variance."""
        self.compute_P_X_given_Z(n)  # Ensure we use the updated P(X | Z)

        total_variance_within = 0
        total_variance_between = 0
        overall_mean = sum(self.group_stats[w]["Y_hat"] * self.group_stats[w]["P"] for w in self.group_stats)

        for w in self.group_stats:
            # Within-group variance
            if self.group_stats[w]["nx"] != 0:
                variance_within = (self.group_stats[w]["sum_squared_Y"] / self.group_stats[w]["nx"]) - (self.group_stats[w]["Y_hat"]) ** 2
            else:
                variance_within = 0
            total_variance_within += self.group_stats[w]["P"] * variance_within

            # Between-group variance
            variance_between = (self.group_stats[w]["Y_hat"] - overall_mean) ** 2
            total_variance_between += self.group_stats[w]["P"] * variance_between

        total_variance = total_variance_within + total_variance_between
        standard_error = math.sqrt(total_variance / n)
        margin_of_error = self.z_score * standard_error
        lower_bound = overall_mean - margin_of_error
        upper_bound = overall_mean + margin_of_error
        return overall_mean, lower_bound, upper_bound, margin_of_error

class EffecientMAROneRelation:
    def __init__(self, input_file, target, group, Z_condition =[None], confidence_level=0.95, margin_of_error=0.01):
        self.input_file = input_file
        self.group = group[0]
        self.target = target[1]
       # self.Z_condition = Z_condition[0]  # Add Z_condition for filtering
        self.Z_condition = [Z_condition[0]] if Z_condition else []
        self.confidence_level = confidence_level
        self.margin_of_error = margin_of_error
        self.min_samples = None
        self.output_file= "Mar_CI_results/MARCI_"+input_file.rsplit(".", 1)[0].split("/")[-1]+"_runningCI.txt"
        self.condition_attr=[]
        self.conditionOperator=[] 
        self.valueOfZcondition=[]
        if self.Z_condition  : #" 'Z == 2"
          
          for z in self.Z_condition:
              temp= z.split()
              self.condition_attr.append(temp[0])
              self.conditionOperator.append(temp[1])
              self.valueOfZcondition.append(temp[2])
        # Initialize confidence interval calculator
        self.CI = TotalVarianceCI(confidence_level,self.condition_attr, self.conditionOperator, self.valueOfZcondition)

        # Initialize the text file by writing a header
        with open(self.output_file, 'a') as txtfile:
            txtfile.write("---------------------------------------\n")
            txtfile.write("Count\tMean\tLower Bound\tUpper Bound\n")

    def getExpectation(self,stopEarlyAt=None, stopEarly=True):
        # Process the CSV file and get the final result
        final_result, (lb, ub) = self.process_stream(stopEarlyAt, stopEarly)
        return final_result, (lb, ub)

    def process_stream(self, stopEarlyAt=None, stopEarly=False):
        n, group_stats = 0, {}

        # Open the file and process it using the csv module
        with open(self.input_file, 'r') as file:
            reader = csv.reader(file)  # Use csv.reader to handle quoted commas
            header = next(reader)  # Read the header row

           # total_rows = sum(1 for _ in file)  # Count total rows in file
            #print("Total rows in file:", total_rows)

            # Define a proportional interval for saving based on total_rows
            #save_interval = max(1, total_rows // 5)  # Save every 20% of the file size
            #file.seek(0)

           # header = next(reader)  # Read the header row again after resetting the file position
            target_idx = header.index(self.target)
            group_idxs = [header.index(g) for g in self.group]
            if self.Z_condition:
                Z_idx =[]
                for h in self.condition_attr:
                    Z_idx.append(header.index(h))
               # Z_idx = header.index(self.condition_attr)  # Add Z index for conditional expectation

            file.seek(0)
            next(reader)  # Skip header again
            Z_value=[-1] # just for the case where there is no condtion
            for row in reader:
                A_o = tuple(row[idx] for idx in group_idxs)  # Update the group regardless of whether Y is missing
               # Z_vals=[]
                if self.Z_condition:
                    Z_value = [row[z] for z in Z_idx]
                n += 1
               # print("n =",n)
                if row[target_idx] == '':
                    for i in range (0,len(Z_value)):
                        if Z_value[i] == -1: 
                              temp = None
                        else: 
                            temp = self.valueOfZcondition[i]
                        self.CI.update(None, A_o, Z_value[i], Z_condition=temp , misc_flag=True)
                    #self.CI.getCurrentExpectation(n)
                    continue  # Skip rows with missing target values

                #Y = float(row[target_idx])
                try:
                    Y = float(row[target_idx])
                except (ValueError, TypeError):
                    Y = None
                #n += 1
                for i in range (0,len(Z_value)):
                        if Z_value[i] == -1: 
                              temp = None
                        else: 
                              temp = self.valueOfZcondition[i]
                        self.CI.update(Y, A_o, Z_value[i], Z_condition=temp , misc_flag=True)
                
                # Stopping Early
                if stopEarlyAt is not None and n >= stopEarlyAt:
                    print(f"Early stopping: 1% of the data ({n} rows) has been processed.")
                    val, lb, ub, margin_of_error = self.CI.compute_confidence_interval(n)
                    self.save_to_txt(n, val, lb, ub)
                    return val, (lb, ub)
# =============================================================================
#                 if n <= total_rows: # just for debugging
#                     self.CI.getCurrentExpectation(n)
# =============================================================================

        final_value, lower_bound, upper_bound, margin_of_error = self.CI.compute_confidence_interval(n)
        self.save_to_txt(n, final_value, lower_bound, upper_bound)
        return final_value, (lower_bound, upper_bound)

    def save_to_txt(self, count, mean, lower_bound, upper_bound):
        with open(self.output_file, 'a') as txtfile:
            txtfile.write(f"{count}\t{mean}\t{lower_bound}\t{upper_bound}\n")
class MARRippleJoin:
    def __init__(self, relation1File, relation2File, joinCol1, joinCol2, predicate, attr, group, Z_condition=[], ConfidenceLevel=0.95, margin_of_error=0.2, tolerance=0.001):
        self.csv_file1 = relation1File
        self.csv_file2 = relation2File
        self.column_r = joinCol1
        self.column_s = joinCol2
        self.operator = predicate
        self.attribute = attr[1]
        self.aggregation_func = attr[0]
        self.group = group[0]  # Fix the group to a single column
        self.confidence_level = ConfidenceLevel
        self.margin_of_error = margin_of_error
        self.tolerance = tolerance
        self.Z_condition = Z_condition[0] if Z_condition else None
        self.condition_attr, self.conditionOperator, self.valueOfZcondition = self.parse_condition(self.Z_condition)
        self.CI = TotalVarianceCI(self.confidence_level, self.condition_attr, self.conditionOperator, self.valueOfZcondition)
        
        # add output dir here and uncomment entire block
        self.output_file= "Mar_CI_results/MARCI_Join_"+relation1File.rsplit(".", 1)[0].split("/")[-1]+"_runningCI.txt"
        # Initialize the text file by writing a header
        with open(self.output_file, 'a') as txtfile:
            txtfile.write("---------------------------------------\n")
            txtfile.write("Count\tMean\tLower Bound\tUpper Bound\n")
            
    def parse_condition(self, Z_condition):
        if Z_condition:
            return Z_condition.split()
        return None, None, None

    def predicate(self, value_r, value_s):
        if not value_r or not value_s:  # Handles empty strings and None
            return False
        if self.operator == '==':
            return value_r == value_s
        elif self.operator == '>':
            return value_r > value_s
        elif self.operator == '<':
            return value_r < value_s
        else:
            raise ValueError("Unsupported operator")

    def ripple_join(self,stopEarlyAt=None):
        processed_count = 0
        results_R = []
        results_S = []
        max_value = 1
        stop_condition = False
        n = 0
        stop=0
        myFlag = True
        with open(self.csv_file1, 'r') as file_r, open(self.csv_file2, 'r') as file_s:
            reader_r = list(csv.DictReader(file_r))
            reader_s = list(csv.DictReader(file_s))
            len_r = len(reader_r)
            # assume the outer relation is the laregest and it has the attribute we want to aggregate
            while not stop_condition:
                if max_value > len(reader_r):
                    break # Safety condition to avoid infinite loop
                #print("Iteration: ", max_value)
    
                # To store matches found in this iteration
                matches_in_iteration = []
    
                # First loop: Compare R[i] with S[max] for i = 1 to max-1
                for i in range(0, max_value - 1):
                    r_value = reader_r[i][self.column_r]
                    if (max_value )> len(reader_s) :
                        s_value = None
                        s_attribute_value = None
                    else:
                        s_value = reader_s[max_value - 1][self.column_s]
                        s_attribute_value = reader_s[max_value - 1][self.attribute] if self.attribute in reader_s[max_value - 1] else None
                    r_group = reader_r[i][self.group[0]]
                    r_attribute_value = reader_r[i][self.attribute] if self.attribute in reader_r[i] else None
                   # s_attribute_value = reader_s[max_value - 1][self.attribute] if self.attribute in reader_s[max_value - 1] else None
                    if i == (max_value-2) and myFlag:
                        self.CI.update(None, r_group, misc_flag=True)
                        #n+=1
                    if self.predicate(r_value, s_value):
                        matches_in_iteration.append((reader_r[i], reader_s[max_value - 1]))
                        if r_attribute_value !='':
                             self.CI.update(float(r_attribute_value), r_group, misc_flag=False)
                # Second loop: Compare R[max] with S[i] for i = 1 to max
                for i in range(0, max_value):
                    r_value = reader_r[max_value - 1][self.column_r]
                    if i == len(reader_s) :
                        s_value = None
                        s_attribute_value = None
                    else:
                        s_value = reader_s[i][self.column_s]
                        s_attribute_value = reader_s[i][self.attribute] if self.attribute in reader_s[i] else None
                    r_group = reader_r[max_value - 1][self.group[0]]  # Access the group column correctly
                    r_attribute_value = reader_r[max_value - 1][self.attribute] if self.attribute in reader_r[max_value - 1] else None
                    #s_attribute_value = reader_s[i][self.attribute] if self.attribute in reader_s[i] else None
                    if i == (max_value-1) :
                        myFlag= False
                        self.CI.update(None, r_group, misc_flag=True)
                        n+=1
                    if self.predicate(r_value, s_value):
                        matches_in_iteration.append((reader_r[max_value - 1], reader_s[i]))
                        if r_attribute_value !='':
                            self.CI.update(float(r_attribute_value), r_group, misc_flag=False)
                    
                    # stopping early
                    stop+=1    
                    if stopEarlyAt is not None and stop >= stopEarlyAt:
                        print(f"Early stopping: 1% of the data ({stop} rows) has been processed.")
                        val, lb, ub, margin_of_error = self.CI.compute_confidence_interval(n)
                        self.save_to_txt(n, val, lb, ub)
                        return val, (lb, ub)
                    
# =============================================================================
#                 # Output all matches found in the iteration
#                 if matches_in_iteration:
#                     print(f"Matches found in iteration {max_value}:")
#                     for match in matches_in_iteration:
#                         print(f"Match: {match[0]} matches {match[1]}")
#                 else:
#                     print(f"No matches found in iteration {max_value}")
# =============================================================================
                #self.CI.getCurrentExpectation(n) # just for debugging 
                max_value += 1
               # n+=1

        val, lb, ub, margin_of_error = self.CI.compute_confidence_interval(n)
        self.save_to_txt(n, val, lb, ub)
        return val, (lb, ub)     
    def save_to_txt(self, count, mean, lower_bound, upper_bound):
        with open(self.output_file, 'a') as txtfile:
            txtfile.write(f"{count}\t{mean}\t{lower_bound}\t{upper_bound}\n")

class EffecientMARJoins:  # initial implememntation was nested loops jon 
    def __init__(self, relation1File, relation2File, joinCol1, joinCol2, predicate, attr, group, Z_condition =[None], attribute_relation='relation1', confidence_level=0.95, margin_of_error=0.01):
        self.csv_file1 = relation1File
        self.csv_file2 = relation2File
        self.column_r = joinCol1
        self.column_s = joinCol2
        self.operator = predicate  # Predicate is the operator, not the method name
        self.attribute = attr[1]
        self.group = group[0]
        self.attribute_relation = attribute_relation  
        self.confidence_level = confidence_level
        self.margin_of_error = margin_of_error
        self.min_samples = None
        self.CI = TotalVarianceCI(confidence_level)
        self.total_r = 0  # Track total rows processed from R
        self.total_s = 0  # Track total rows processed from S
        self.misc_flag = False
        self.joinedRelation = []
        self.Z_condition = Z_condition[0]  # Add Z_condition for filtering
        self.condition_attr=None # in the where calsue
        self.conditionOperator=None 
        self.valueOfZcondition=None
        if self.Z_condition :
            self.condition_attr, self.conditionOperator, self.valueOfZcondition = self.Z_condition.split()
        self.CI = TotalVarianceCI(confidence_level,self.condition_attr, self.conditionOperator, self.valueOfZcondition)
        

    def nested_loops_join(self, stopEarly=False):
        """Nested loop join between relation R and S using csv.reader for fast correct parsing."""
        with open(self.csv_file1, 'r') as file_r, open(self.csv_file2, 'r') as file_s:
            # Use csv.reader to handle quoted commas
            reader_r = csv.reader(file_r)
            reader_s = csv.reader(file_s)
            total_rows = sum(1 for _ in file_r) -1  # Count total rows in file
       

            # Define a proportional interval for saving based on total_rows
            save_interval = max(1, total_rows // 5)  # Save every 20% of the file size
            file_r.seek(0)
            # Read the header row of relation R and S
            header_r = next(reader_r)  # Skip the header
            header_s = next(reader_s)  # Skip the header
            # max_iter= (file_r.tell() * file_s.tell())
            
                
           
            if self.column_r not in header_r:
                temp = file_r
                file_r=file_s
                file_s=temp
                temp2=  header_r
                header_r=header_s
                header_s=temp2
                ######
                file_r.seek(0) 
                file_s.seek(0) 
                reader_r = csv.reader(file_r)
                reader_s = csv.reader(file_s)

            # Map column names to indices
            # Map column names to indices
            if self.attribute in header_r and self.attribute in header_s:
                self.attribute_relation = 'both'
            elif self.attribute in header_s:
                self.attribute_relation = 'relation2'
                
            idx_r = header_r.index(self.column_r)
            idx_s = header_s.index(self.column_s)
    
            group_indices_r = []
            group_indices_s = []
           
            if self.attribute_relation == 'both':
                group_indices_r = [header_r.index(g) for g in self.group]
                group_indices_s = [header_s.index(g) for g in self.group]
            elif self.attribute_relation == 'relation2':
                group_indices_s = [header_s.index(g) for g in self.group]
                if self.Z_condition:
                    Z_idx = header_s.index(self.condition_attr)  # Add Z index for conditional expectation
            else:
                group_indices_r = [header_r.index(g) for g in self.group]
                if self.Z_condition:
                    Z_idx = header_r.index(self.condition_attr)  # Add Z index for conditional expectation
    
            attr_idx_r = header_r.index(self.attribute) if self.attribute in header_r else None
            attr_idx_s = header_s.index(self.attribute) if self.attribute in header_s else None
            m = 0  # Track the number of valid joined samples
    
            # Reset file pointers after counting rows
            len_r = sum(1 for _ in file_r)  # Count rows in relation R
            file_r.seek(0)  # Reset pointer to the beginning of the file
            next(reader_r)  # Skip header again after resetting the pointer
    
            len_s = sum(1 for _ in file_s)  # Count rows in relation S
            file_s.seek(0)  # Reset pointer to the beginning of the file
            next(reader_s)  # Skip header again after resetting the pointer
            len_r -= 1
            len_s -= 1
            max_iter = len_r * len_s
    
            # Initial sample for variance estimation
            if stopEarly:
                initial_sample_size = min(30, min(len_r, len_s))
                initial_sample = []
                try:
                    if self.attribute_relation == 'relation2':
                        if self.Z_condition: # if there is a condtion
                            for _ in range(initial_sample_size):
                                line = next(reader_s)
                                # Skip empty or non-numeric values
                                
                                if (attr_idx_s is not None and line[attr_idx_s] != '' and line[attr_idx_s].replace('.', '', 1).isdigit()) or line[Z_idx] == str(self.valueOfZcondition):
                                    if line[attr_idx_s] == '' : continue
                                    initial_sample.append(float(line[attr_idx_r]))
                        else: #no condtion 
                            for _ in range(initial_sample_size):
                                line = next(reader_s)
                                # Skip empty or non-numeric values
                                
                                if (attr_idx_s is not None and line[attr_idx_s] != '' and line[attr_idx_s].replace('.', '', 1).isdigit()):
                            
                                    initial_sample.append(float(line[attr_idx_r]))
                    else: # in relation1
                        if self.Z_condition: # if there is a condtion
                            for _ in range(initial_sample_size):
                                line = next(reader_r)
                                # Skip empty or non-numeric values
                                
                                if (attr_idx_r is not None and line[attr_idx_r] != '' and line[attr_idx_r].replace('.', '', 1).isdigit()) or line[Z_idx] == str(self.valueOfZcondition):
                                    if line[attr_idx_r] == '' : continue
                                    initial_sample.append(float(line[attr_idx_r]))
                        else: #no condtion 
                            for _ in range(initial_sample_size):
                                line = next(reader_s)
                                # Skip empty or non-numeric values
                                
                                if (attr_idx_r is not None and line[attr_idx_r] != '' and line[attr_idx_r].replace('.', '', 1).isdigit()):
                            
                                    initial_sample.append(float(line[attr_idx_r]))
                except StopIteration:
                    pass
    
                if len(initial_sample) > 0:
                    sigma_estimate = np.std(initial_sample, ddof=1)
                    self.min_samples = self.CI.calculate_min_sample_size(sigma_estimate, self.margin_of_error, max_iter)
                else:
                    self.min_samples = max_iter
    
            # Continue processing rows after skipping the header
            file_r.seek(0)  # Reset pointer to the beginning of the file
            next(reader_r)  # Skip header again after resetting the pointer
            file_s.seek(0)  # Reset pointer to the beginning of the file
            next(reader_s)  # Skip header again after resetting the pointer
            Z_value=-2 
            for row_r in reader_r:
                self.total_r += 1
                self.misc_flag = True
                file_s.seek(0)  # Reset file_s to the start of the file
                #reader_s = csv.reader(file_s)  # Recreate reader after seek
                next(reader_s)  # Skip the header for S after resetting the pointer
                if self.Z_condition:
                    Z_value = row_r[Z_idx]
                    
                for row_s in reader_s:
                    #print(row_s)
                    self.total_s += 1
                    group = self.whatGroupToUpdate(row_r, row_s, group_indices_r, group_indices_s,  Z_value)
                    self.misc_flag = False
                    if row_r[idx_r] == '' or row_s[idx_s] == '':
                        continue
    
                    if self.check_predicate(row_r[idx_r], row_s[idx_s]):
                        self.process_joined_row(row_r, row_s, group, attr_idx_r, attr_idx_s,Z_value )
                        m += 1
    
                        if self.total_r % save_interval == 0 or (stopEarly and self.min_samples is not None and m >= self.min_samples):
                            current_value, lower_bound, upper_bound, margin_of_error = self.CI.compute_confidence_interval(self.total_r)
                            if stopEarly and margin_of_error <= self.margin_of_error:
                                print(f"Stopping early with CI: [{lower_bound:.4f}, {upper_bound:.4f}] at sample {self.total_r}.")
                                return current_value, (lower_bound, upper_bound)
                if self.total_r <= len_r:
                    self.CI.getCurrentExpectation(self.total_r)
    
       
            final_value, lower_bound, upper_bound, margin_of_error = self.CI.compute_confidence_interval(self.total_r)
            return final_value, (lower_bound, upper_bound)


    def check_predicate(self, value_r, value_s):
        """Check the condition for the join."""
        if self.operator == '==':
            return value_r == value_s
        elif self.operator == '>':
            return value_r > value_s
        elif self.operator == '<':
            return value_r < value_s
        else:
            raise ValueError("Unsupported operator")

    def process_joined_row(self, row_r, row_s, group, attr_idx_r, attr_idx_s,Z_value):
        """Process the joined rows, updating the Y value based on the attribute relation."""
        self.joinedRelation.append([row_r, row_s])

        if self.attribute_relation == 'relation1':
            Y = 0 if attr_idx_r is None or pd.isna(row_r[attr_idx_r]) or row_r[attr_idx_r] == '' else float(row_r[attr_idx_r])
            self.CI.update(Y, group, Z_value, self.valueOfZcondition,self.misc_flag, flag=False)
        elif self.attribute_relation == 'relation2':
            Y = 0 if attr_idx_s is None or pd.isna(row_s[attr_idx_s]) or row_s[attr_idx_s] == '' else float(row_s[attr_idx_s])
            self.CI.update(Y, group,Z_value, self.valueOfZcondition, self.misc_flag, flag=False)
        elif self.attribute_relation == 'both':
            Y_r = 0 if attr_idx_r is None or pd.isna(row_r[attr_idx_r]) or row_r[attr_idx_r] == '' else float(row_r[attr_idx_r])
            Y_s = 0 if attr_idx_s is None or pd.isna(row_s[attr_idx_s]) or row_s[attr_idx_s] == '' else float(row_s[attr_idx_s])
            Y = (Y_r + Y_s) / 2
            self.CI.update(Y, group, Z_value, self.valueOfZcondition,self.misc_flag, flag=False)
        else:
            raise ValueError("Invalid attribute_relation.")

    def whatGroupToUpdate(self, df_r_row, df_s_row, group_indices_r, group_indices_s, Z_value):
        """Update the group based on relation (relation1, relation2, or both)."""
        if self.attribute_relation == 'relation1':
            A_o_r = tuple(df_r_row[i] for i in group_indices_r)
            self.CI.update(None, A_o_r,Z_value, self.valueOfZcondition , self.misc_flag)
            return A_o_r
        elif self.attribute_relation == 'relation2':
            A_o_s = tuple(df_s_row[i] for i in group_indices_s)
            self.CI.update(None, A_o_s, Z_value, self.valueOfZcondition , self.misc_flag)
            return A_o_s
        else:
            A_o_r = tuple(df_r_row[i] for i in group_indices_r)
            A_o_s = tuple(df_s_row[i] for i in group_indices_s)
            self.CI.update(None, A_o_r + A_o_s, Z_value, self.valueOfZcondition , self.misc_flag)
            return A_o_r + A_o_s



