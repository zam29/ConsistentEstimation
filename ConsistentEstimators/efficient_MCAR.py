#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""


Contains classes to handle MCAR efficiently 
- one relation 
- relation joins (ripple join is implemented )

chunk reader is used for speed 
chosen chunk size affects the speed, so choose a size that suits the input data size
"""


import pandas as pd
import numpy as np
from scipy.stats import norm
import time
import csv
import os

output_dir = "Mcar_CI_results"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)
    
class timeBuckets:
    def __init__(self):
        self.time_dicts = {}
        self.current_time = 0
        self.prev_time = 0
        self.current_time_delta = 0
        self.location = 0

    def record(self, bucket_name: str):
        self.current_time = time.time()
        if self.prev_time == 0:
            self.prev_time = self.current_time
        self.current_time_delta = self.current_time - self.prev_time
        old_val = self.time_dicts.get(bucket_name)
        if old_val is None:
            # This is a new bucket
            new_bucket = {
                "time_elapsed": self.current_time_delta,
                "number_of_times_run": 1
            }
            self.time_dicts[bucket_name] = new_bucket
        else:
            self.time_dicts[bucket_name]["time_elapsed"] += self.current_time_delta
            self.time_dicts[bucket_name]["number_of_times_run"] += 1
        self.prev_time = self.current_time

    def printAll(self):
        sumvals = 0
        for key, val in self.time_dicts.items():
            print(key, val)
            sumvals += val["time_elapsed"]
        print("----------------")
        print("total time in buckets", sumvals)
        print("----------------")


timer = timeBuckets()


import pandas as pd
import numpy as np
from scipy.stats import norm

class IncrementalConfidenceIntervals:
    def __init__(self, aggregation_func, confidence_level):
        self.AggType = aggregation_func.lower()
        self.val1_score = norm.ppf((1 + confidence_level) / 2)
        self.mean = 0
        self.sum = 0
        self.count = 0
        self.squared_diff_sum = 0
    def update(self, value):
        self.update_batch(np.array([value]))
    def update_batch(self, values): 
        #timer.record("location 1")

        batch_count = len(values)
        #timer.record("location 2")
        if batch_count == 0:
            return
        #timer.record("location 4")
        if self.AggType == 'avg':
            old_mean = self.mean
            self.count += batch_count
            self.mean += (values.sum() - batch_count * old_mean) / self.count
            self.squared_diff_sum += ((values - old_mean) * (values - self.mean)).sum()
        
        elif self.AggType == 'sum':
            self.sum += values.sum()
            self.count += batch_count

    def compute_ci(self):
        if self.count < 2:
            return np.nan, np.nan, np.nan

        if self.AggType == 'avg':
            variance = self.squared_diff_sum / (self.count - 1)
            sem = np.sqrt(variance) / np.sqrt(self.count)
            margin_of_error = self.val1_score * sem
            return self.mean, self.mean - margin_of_error, self.mean + margin_of_error

        if self.AggType == 'sum':
            sem = np.sqrt(self.squared_diff_sum / (self.count - 1)) * np.sqrt(self.count)
            margin_of_error = self.val1_score * sem
            return self.sum, self.sum - margin_of_error, self.sum + margin_of_error

        if self.AggType == 'count':
            margin_of_error = self.val1_score * np.sqrt(self.count)
            return self.count, self.count - margin_of_error, self.count + margin_of_error

class oneRelationMCAR:
    def __init__(self, relationFile, attr, conditions=[], ConfidenceLevel=0.95, margin_of_error=0.1):
        self.relationFile = relationFile
        self.attribute = attr[1]
        self.aggregation_func = attr[0]
        self.conditions = conditions
        self.confidence_level = ConfidenceLevel
        self.margin_of_error = margin_of_error
  
       
# =============================================================================
        self.output_file= "Mcar_CI_results/MCARCI_"+relationFile.rsplit(".", 1)[0].split("/")[-1]+"1percent_runningCI.txt"
        with open(self.output_file, 'a') as txtfile:
             txtfile.write("threshold\tMean\tLower Bound\tUpper Bound\n")
# =============================================================================


    def parse_conditions(self):
        #timer.record("location 15")
        self.parsed_conditions = []
        sample_df = pd.read_csv(self.relationFile, nrows=1)  # Read a small sample to determine types
    
        for condition in self.conditions:
            if '>' in condition:
                col, value = condition.split('>')
                operator = '>'
            elif '<' in condition:
                col, value = condition.split('<')
                operator = '<'
            elif '==' in condition:
                col, value = condition.split('==')
                operator = '=='
            else:
                raise ValueError(f"Unsupported condition: {condition}")
            
            col = col.strip()
            value = value.strip()
    
            # Determine the column type from the sample data
            if pd.api.types.is_numeric_dtype(sample_df[col]):
                col_type = 'numeric'
            else:
                col_type = 'string'
    
            # Store the parsed condition as (col_name, operator, value, col_type)
            self.parsed_conditions.append((col, operator, value, col_type))
    
       # timer.record("location 16")


    def build_combined_mask(self, df):
       # timer.record("location 34")
        mask = np.ones(len(df), dtype=bool)
    
        for col, operator, value, col_type in self.parsed_conditions:
            if value.isnumeric() and col_type == 'numeric':
                value = float(value)  # Convert the condition value to numeric
                # Apply numeric comparisons
                if operator == '==':
                    mask &= (df[col] == value)
                elif operator == '>':
                    mask &= (df[col] > value)
                elif operator == '<':
                    mask &= (df[col] < value)
                else:
                    raise ValueError(f"Unsupported operator for numeric comparison: {operator}")
    
            elif col_type == 'string':
                # Handle string comparisons
                if operator == '==':
                    mask &= (df[col].astype(str) == value)
                    #mask &= (df[col] == value)
                else:
                    raise ValueError(f"Unsupported operator for string comparison: {operator}")
    
       # timer.record("location 35")
        return mask

    def aggregate(self, stopEarlyAt=None, chunk_size=500000):
        # Parse conditions only once
        self.parse_conditions()
        self.CI = IncrementalConfidenceIntervals(self.aggregation_func, self.confidence_level)
        
        if stopEarlyAt is not None: 
            threshold = stopEarlyAt
            chunk_size = (threshold / 2)
            
        cols_to_use = [self.attribute] + [cond[0] for cond in self.parsed_conditions]
        
        processed_count = 0
        all_values = []  # A list to collect all valid values
        
        for chunk in pd.read_csv(self.relationFile, chunksize=chunk_size, usecols=cols_to_use):
            # Apply all conditions using combined mask
            processed_count += len(chunk)
            if self.parsed_conditions:
                mask = self.build_combined_mask(chunk)
                chunk = chunk[mask]

            # Drop NaN values and collect valid data for the specified attribute
            chunk_values = chunk[self.attribute].dropna().values.astype(float)
            
            # Append valid chunk values to the list
            all_values.append(chunk_values)
            processed_count += len(chunk_values)

            # If processed count exceeds threshold, stop early
            if stopEarlyAt is not None and processed_count >= threshold:
                print(f"Early stopping: 1% of the data ({processed_count} rows) has been processed.")
                
                # Concatenate all chunk values into a single numpy array
                concatenated_values = np.concatenate(all_values)
                self.CI.update_batch(concatenated_values)
                val, lb, ub = self.CI.compute_ci()
                self.save_to_txt(threshold, val, lb, ub)
                return val, (lb, ub)

        # Process remaining data after the loop
        if all_values:
            concatenated_values = np.concatenate(all_values)
            self.CI.update_batch(concatenated_values)

        val, lb, ub = self.CI.compute_ci()
        self.save_to_txt( processed_count, val, lb, ub)
        return val, (lb, ub)

    
    def save_to_txt(self, count, mean, lower_bound, upper_bound):
        with open(self.output_file, 'a') as txtfile:
            txtfile.write(f"{count}\t{mean}\t{lower_bound}\t{upper_bound}\n")

    

# =============================================================================
# csv_file = 'income_clean.csv'
# attribute = ['AVG','income']
# conditions = ['year == 2018']  # 'average', 'sum', or 'count'
# confidence_level = 0.95  # 95% confidence interval
# 
# aggregator = oneRelationMCAR(csv_file, attribute,conditions)
# final_aggregate, confidence_interval = aggregator.aggregate()
# 
# print(f"Final of attribute: {final_aggregate}")
# print("Confidence interval:", confidence_interval)
# =============================================================================



    
    
class MCARRippleJoin:
    def __init__(self, relation1File, relation2File, joinCol1, joinCol2, predicate, attr, ConfidenceLevel=0.95, margin_of_error=0.2, tolerance=0.001, chunk_size=500000):
        self.csv_file1 = relation1File
        self.csv_file2 = relation2File
        self.column_r = joinCol1
        self.column_s = joinCol2
        self.operator = predicate
        self.attribute = attr[1]
        self.aggregation_func = attr[0]
        self.confidence_level = ConfidenceLevel
        self.margin_of_error = margin_of_error
        self.tolerance = tolerance
        self.chunk_size = chunk_size
        self.output_file= "Mcar_CI_results/MARCI_Join_"+relation1File.rsplit(".", 1)[0].split("/")[-1]+"_runningCI.txt"
        self.CI = IncrementalConfidenceIntervals(self.aggregation_func, self.confidence_level)
        with open(self.output_file, 'a') as txtfile:
             txtfile.write("threshold\tMean\tLower Bound\tUpper Bound\n")
    def predicate(self, value_r, value_s):
        if self.operator == '==':
            return value_r == value_s
        elif self.operator == '>':
            return value_r > value_s
        elif self.operator == '<':
            return value_r < value_s
        else:
            raise ValueError("Unsupported operator")
    def processChunk(self, results_R,results_S):
        # Batch update CI after processing a chunk
        if results_R:
            self.CI.update_batch(np.array(results_R))
            results_R.clear()  # Clear after batch update

        if results_S:
            self.CI.update_batch(np.array(results_S))
            results_S.clear()  # Clear after batch update
    def ripple_join(self,stopEarlyAt=None): 
      
        if stopEarlyAt is not None: 
            threshold = stopEarlyAt
            self.chunk_size = (threshold / 2)
        #print(f"Total rows: {total_rows}, 1% threshold: {threshold}")

        # Read chunks from R
        #timer.record("location 4")
        r_chunks = pd.read_csv(self.csv_file1, chunksize=self.chunk_size, usecols=[self.column_r, self.attribute])
        #timer.record("location 5")
        processed_count = 0  # Track how many rows we've processed

        results_R = []
        results_S = []

        # Process chunks from R
        #timer.record("location 6")
        for r_chunk in r_chunks:
            r_values = r_chunk[self.column_r].values
            attribute_values_R = r_chunk[self.attribute].values if self.attribute in r_chunk.columns else np.full(len(r_chunk), np.nan)

            # Re-create iterator for reading chunks from S for each chunk of R
            s_chunks = pd.read_csv(self.csv_file2, chunksize=self.chunk_size, usecols=[self.column_s])

            # Process chunks from S
            for s_chunk in s_chunks:
                s_values = s_chunk[self.column_s].values
                attribute_values_S = s_chunk[self.attribute].values if self.attribute in s_chunk.columns else np.full(len(s_chunk), np.nan)

                # Compare R[i] with S[max] 
                max_s_idx = len(s_values) - 1  
                for i in range(len(r_values)):
                    if self.predicate(r_values[i], s_values[max_s_idx]):  # Only compare with S[max_s_idx]
                        if not np.isnan(attribute_values_R[i]):
                            results_R.append(attribute_values_R[i])
                        elif not np.isnan(attribute_values_S[max_s_idx]):
                            results_S.append(attribute_values_S[max_s_idx])

                # Compare S[i] with R[max] 
                max_r_idx = len(r_values) - 1  
                for i in range(len(s_values)):
                    if self.predicate(r_values[max_r_idx], s_values[i]):  # Only use R[max_r_idx]
                        if not np.isnan(attribute_values_R[max_r_idx]):
                            results_R.append(attribute_values_R[max_r_idx])
                        elif not np.isnan(attribute_values_S[i]):
                            results_S.append(attribute_values_S[i])

                    processed_count += 1
                    # if processed_count % 10000 == 0:
                    #   print(f"Processed {processed_count} comparisons out of {threshold}.")

                    if stopEarlyAt is not None and processed_count >= threshold:
                        print(f"Early stopping: 1% of the data ({processed_count} rows) has been processed.")
                        #timer.record("location 9")
                        self.processChunk(results_R, results_S)
                        #timer.record("location 10")
                        val, lb, ub = self.CI.compute_ci()
                        #timer.record("location 11")
                        #timer.printAll()
                        self.save_to_txt( threshold, val, lb, ub)
                        return val, (lb, ub)
        #timer.record("location 9")
        # Final confidence interval and result if threshold is not reached
        self.processChunk(results_R, results_S)
        #timer.record("location 10")
        val, lb, ub = self.CI.compute_ci()
        #timer.record("location 11")
        #timer.printAll()
        #print("here")
        
        self.save_to_txt( processed_count, val, lb, ub)
        return val, (lb, ub)
    def save_to_txt(self, count, mean, lower_bound, upper_bound):
        with open(self.output_file, 'a') as txtfile:
            txtfile.write(f"{count}\t{mean}\t{lower_bound}\t{upper_bound}\n")



            
            

# =============================================================================
# relation1File = 'r_head.csv'
# relation2File = 's_head.csv'
# joinCol1 = 'ID'  # Replace with the actual column name in R
# joinCol2 = 'ID'  # Replace with the actual column name in S
# predicate = '>'  # Could be '==', '>', or '<'
# attr = ['AVG','TOTAL_FEE']  # attribute name to aggregate
# ConfidenceLevel = 0.95  # 95% confidence interval
# U = MCARRippleJoin(relation1File, relation2File, joinCol1, joinCol2, predicate, attr, ConfidenceLevel)
# final_aggregate, confidence_interval = U.ripple_join()
# print(final_aggregate)
# =============================================================================

# =============================================================================
# csv_file2 = 'rippleTest1.csv'
# csv_file1 = 'rippleTest2.csv'
# 
# # Read the CSV files into pandas DataFrames
# df1 = pd.read_csv(csv_file1)
# df2 = pd.read_csv(csv_file2)
# 
# # Specify the column to join on (adjust this based on your files)
# join_column = 'id'  # Replace 'ID' with the actual column name in both files
# 
# # Perform the join (inner join as an example)
# merged_df = pd.merge(df1, df2, on=join_column, how='inner')
# print(merged_df.head())
# # Specify the column for which you want to calculate the average (adjust based on your dataset)
# target_column = 'value'  # Replace 'Fee' with the column you want to average
# 
# # Calculate the average of the target column in the joined result
# average_value = merged_df[target_column].mean()
# 
# # Print the average value
# print(f"The average of the '{target_column}' column is: {average_value}")
# =============================================================================

# =============================================================================
# relation1File = 'rippleTest1.csv'
# relation2File = 'rippleTest2.csv'
# joinCol1 = 'id'  # Replace with the actual column name in R
# joinCol2 = 'id'  # Replace with the actual column name in S
# predicate = '<'  # Could be '==', '>', or '<'
# attr = 'value'  # attribute name to aggregate
# aggFunc = 'average'  #  'average', 'sum', or 'count'
# ConfidenceLevel = 0.95  # 95% confidence interval
# 
# U = MCARRippleJoin(relation1File, relation2File, joinCol1, joinCol2, predicate, attr)
# final_aggregate, confidence_interval = U.ripple_join()
# print(final_aggregate)
# =============================================================================
"""
# main
relation1File = 'rippleTest1.csv'
relation2File = 'rippleTest2.csv'
joinCol1 = 'id'  # Replace with the actual column name in R
joinCol2 = 'id'  # Replace with the actual column name in S
predicate = '>'  # Could be '==', '>', or '<'
attr = 'value'  # attribute name to aggregate
aggFunc = 'average'  #  'average', 'sum', or 'count'
ConfidenceLevel = 0.95  # 95% confidence interval

U = MCARRippleJoin(relation1File, relation2File, joinCol1, joinCol2, predicate, attr, aggFunc, ConfidenceLevel)
join_results, final_aggregate, confidence_interval = U.ripple_join()

# Print or save results
for result in join_results:
    print(result)
if aggFunc == 'average':
    print("Final average of attribute:", final_aggregate)
    print("Confidence interval:", confidence_interval)
elif aggFunc == 'sum':
    print("Final sum of attribute:", final_aggregate)
    print("Confidence interval:", confidence_interval)
elif aggFunc == 'count':
    print("Final count of joins:", final_aggregate)
    print("Confidence interval:", confidence_interval)

    
# one relation
csv_file = 'rippleTest1.csv'
attribute = 'B'
aggregation_func = 'average'  # 'average', 'sum', or 'count'
confidence_level = 0.95  # 95% confidence interval

aggregator = oneRelationMCAR(csv_file, attribute, aggregation_func, confidence_level)
final_aggregate, confidence_interval = aggregator.aggregate()

print(f"Final {aggregation_func} of attribute: {final_aggregate}")
print("Confidence interval:", confidence_interval)

"""
# =============================================================================
# import time 
# # one relation
# csv_file = 'rippleTest1.csv'
# attr = ['AVG', 'value']
# conditions = []  # Multiple conditions
# confidence_level = 0.95  # 95% confidence interval
# start_time = time.time()
# aggregator = oneRelationMCAR(csv_file, attr, conditions, confidence_level)
# final_aggregate, confidence_interval = aggregator.aggregate()
# total = time.time() - start_time
# print("total time approach1:", total)
# print(f"Final {attr[0]} of attribute: {final_aggregate}")
# print("Confidence interval:", confidence_interval)
# =============================================================================
# =============================================================================
# csv_file = 'file1.csv'
# attribute = 'B'
# aggregation_func = 'average'  # Could be 'average', 'sum', or 'count'
# confidence_level = 0.95  # 95% confidence interval
# 
# aggregator = oneRelationMCAR(csv_file, attribute, aggregation_func, confidence_level)
# final_aggregate, confidence_interval = aggregator.aggregate()
# 
# print(f"Final {aggregation_func} of attribute: {final_aggregate}")
# print("Confidence interval:", confidence_interval)
# =============================================================================
