#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Oct 10 17:07:02 2024

Stopping early for single relation and relation joins 
MAR case
change the percentage of rows to process
"""

import time
import Multi_UI
import multiprocessing
import os
import  traceback

output_dir = "Mar_StoppingEarly/OneR_1_percent"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)
    
output_dir2 = "Mar_StoppingEarly/Join_1_percent"
if not os.path.exists(output_dir2):
    os.makedirs(output_dir2)
    

    
import re

percentage=0.01

# =============================================================================
# def add_quotes_for_strings(query):
#     # Regular expression to detect string conditions without quotes
#     pattern_without_quotes = r"(\w+)\s*=\s*([^'\s]+)"
#         # Replace conditions without quotes with conditions that include quotes
#     query = re.sub(pattern_without_quotes, r"\1 = '\2'", query)
#     
#     return query
# =============================================================================

def add_quotes_for_strings(query):
    # Regular expression to detect string conditions without quotes (excluding numeric values)
    pattern_without_quotes = r"(\w+)\s*=\s*([^\d'\s]+)"
    
    # Replace string conditions without quotes with conditions that include quotes
    query = re.sub(pattern_without_quotes, r"\1 = '\2'", query)

def run_estimator(estimator, query, return_dict):

    try:
        # Run the estimator and store the result in the shared dictionary
        result = estimator.run([query, "mar"])

        # Check if the result is a tuple (for IntervalEstimator) or a single value (for others)
        if isinstance(result, tuple):
            return_dict['result'] = result  # For IntervalEstimator: handle as tuple (lower_mean, upper_mean)
        else:
            return_dict['result'] = (result, None)  # For single value estimators, use None for the second value
    except Exception as e:
        print(f"Error running estimator: {e}")
        traceback.print_exc() 
        return_dict['result'] = (-11111111, None) 

def run_with_timeout(estimator, query, timeout_seconds=300):
    manager = multiprocessing.Manager()
    return_dict = manager.dict()  # Shared dictionary to store the result
    process = multiprocessing.Process(target=run_estimator, args=(estimator, query, return_dict))

    try:
        process.start()  # Start the process
        process.join(timeout_seconds)  # Wait for the process to complete or timeout

        # If the process is still alive after the timeout, terminate it
        if process.is_alive():
            print(f"Estimator timed out after {timeout_seconds} seconds.")
            process.terminate()  # Terminate the process
            process.join()  # Ensure the process has finished

        # Return the result from the shared dictionary
        if 'result' in return_dict:
            return return_dict['result']
        else:
            print("No result found in return_dict, returning default error value.")
            return (-11111111, None)  # Return special value indicating no result

    except KeyboardInterrupt:
        print("Process interrupted by user.")
        process.terminate()  # Make sure the process is terminated if interrupted
        process.join()  # Ensure the process has finished
        return (-11111111, None)

# Dictionary containing filenames and a list of queries for each file
csv_queries = {
  "rwDatasets/heart_MAR.csv":
      {
        "queries": [
            "Select AVG(BMI) From rwDatasets/heart_2022_with_nans.csv where WeightInKilograms > 160",
            "Select AVG(WeightInKilograms) From rwDatasets/heart_2022_with_nans.csv where BMI < 32",
            "Select AVG(HeightInMeters) From rwDatasets/heart_2022_with_nans.csv",
            
            

            
         
        ],
        "CauseMAR_att": ['Sex'],  # Specify CauseMAR_att for this dataset
        "stop":(445000 * percentage)
    },
    
          "rwDatasets/salaries_MAR.csv":
              {
                "queries": [
                    "Select AVG(BasePay) From rwDatasets/Salaries.csv where Benefits > 44430.12",
                    "Select AVG(Benefits) From rwDatasets/Salaries.csv where BasePay > 76430.12",
                    "Select AVG(BasePay) From rwDatasets/Salaries.csv",
                    

                 
                ],
                "CauseMAR_att": ['JobTitle'], # Specify CauseMAR_att for this dataset
                "stop":(149000 * percentage)
            },

         
    # Add more files and queries here
    "rwDatasets/nyc_MAR.csv": 
        {
    "queries": [

                "Select AVG(passenger_count) From rwDatasets/nyc_MAR_10.0.csv where trip_duration > 400 and store_and_fwd_flag = N",
                "Select AVG(trip_duration) From rwDatasets/nyc_MAR_10.0.csv",
                "Select AVG(trip_duration) From rwDatasets/nyc_MAR_10.0.csv where store_and_fwd_flag = N",

        
     
    ],
    "CauseMAR_att": ['vendor_id'],  # Specify CauseMAR_att for this dataset
    "stop":(729000 * percentage)
}
      
  }
print("Querying MAR  one R data ...")
"""
Querying MCAR data
"""

# Iterate over each file and its list of queries
for filename, file_info in csv_queries.items():
    queries = file_info["queries"]
    CauseMAR_att = file_info["CauseMAR_att"]
    stopEarlyAt= file_info['stop']
    
    # Initialize the estimators with the specific CauseMAR_att for each file
    EffiecientEstimator = Multi_UI.UI(isEfficienMAR=True, CauseMAR_att=CauseMAR_att,stopEarlyAt=stopEarlyAt)

    # Generate output filenames based on the input CSV filename
    timing_output_filename = f"{output_dir}/{filename.split('/')[-1].replace('.csv', '_timing_results.txt')}"
    means_output_filename = f"{output_dir}/{filename.split('/')[-1].replace('.csv', '_means_results.txt')}"

    # Using a single `with` block to handle both output files
    with open(timing_output_filename, "a") as timing_file, \
         open(means_output_filename, "a") as means_file:
    
        for query in queries:
      
            # Calculate the true mean for the specific attribute in the query
# =============================================================================
#             attribute = query.split("AVG(")[1].split(")")[0]  # Extract the attribute name from the query
#             trueMean = trueData[attribute].mean()
#             print("true mean is ",trueMean )
# =============================================================================
            
            # Write the query and headers to the timing file
            timing_file.write(f"\n{query}\n")
            timing_file.write("efficientApproach_Time\n")
            
            # Write the query and headers to the means file
            #means_file.write(f"\n{query} | True mean is {trueMean:.4f}\n")
            means_file.write(f"\n{query} | \n")
            means_file.write("EfficientApproach_Mean\n")
            
            # Measure time and capture the estimated means for each approach
            print()
            print(f"Processing {filename} with query: {query}")
            print()
            print("--------Sample-Based Stop Early------------------")
            start_time = time.time()
            #approach3_mean = EffiecientEstimator.run([query, "mar"])
            approach3_mean,_ = run_with_timeout(EffiecientEstimator, query)
            approach3_time = time.time() - start_time
            print("effecient time:"+str(approach3_time))
            print("effecient mean:"+str(approach3_mean))
            print()
            # If any of the estimators timed out, handle None results
            if approach3_mean == -11111111:
               # approach3_time = float('inf')  # Indicate timeout in time results
                print("Efficient approach timed out.")

            # Write timing results to the timing file
            timing_file.write(f"{approach3_time:.4f}\n")
            
            # Write the estimated means to the means file
            means_file.write(f"{approach3_mean:.4f}\n")


print("-----------------------------------------------------------JOIN QUERIES ----------------------------------------------------")
# Dictionary containing filenames and a list of queries for each file
csv_queries = {
  "rwDatasets/heart_MAR.csv":
      {
        "queries": [
            "Select AVG(BMI) From MarJoinsData/heart1_mar_joins1.csv join MarJoinsData/heart1_mar_joins1.csv, MarJoinsData/heart2_mar_joins2.csv on heart1_mar_joins1.csv.person_id = heart2_mar_joins2.csv.person_id",
            "Select AVG(WeightInKilograms) From MarJoinsData/heart1_mar_joins1.csv join MarJoinsData/heart1_mar_joins1.csv, MarJoinsData/heart2_mar_joins2.csv on heart1_mar_joins1.csv.person_id = heart2_mar_joins2.csv.person_id",

         
        ],
        "CauseMAR_att": ['Sex'],  # Specify CauseMAR_att for this dataset
        "stop":(445000 *2* percentage)
    },
    
          "rwDatasets/salaries_MAR.csv":
              {
                "queries": [
                    "Select AVG(BasePay) From MarJoinsData/salaries2_mar_joins2.csv join MarJoinsData/salaries2_mar_joins2.csv, MarJoinsData/salaries1_mar_joins1.csv on salaries2_mar_joins2.csv.Id = salaries1_mar_joins1.csv.Id",
                 "Select AVG(TotalPay) From MarJoinsData/salaries2_mar_joins2.csv join MarJoinsData/salaries2_mar_joins2.csv, MarJoinsData/salaries1_mar_joins1.csv on salaries2_mar_joins2.csv.Id = salaries1_mar_joins1.csv.Id",
 
                ],
                "CauseMAR_att": ['JobTitle'], # Specify CauseMAR_att for this dataset
                "stop":(149000 *2* percentage)
            },

         
    # Add more files and queries here
    "rwDatasets/nyc_MAR.csv": 
        {
    "queries": [

            "Select AVG(passenger_count) from MarJoinsData/nyc1.csv join MarJoinsData/nyc1.csv, MarJoinsData/nyc2.csv on nyc1.csv.id = nyc2.csv.id",
            "Select AVG(trip_duration) from MarJoinsData/nyc1.csv join MarJoinsData/nyc1.csv, MarJoinsData/nyc2.csv on nyc1.csv.id = nyc2.csv.id",

        
     
    ],
    "CauseMAR_att": ['vendor_id'],  # Specify CauseMAR_att for this dataset
    "stop":(729000 * 2* percentage)
}
        }

print("Querying MAR  join R data ...")
"""
Querying MCAR data
"""

# Iterate over each file and its list of queries
for filename, file_info in csv_queries.items():
    queries = file_info["queries"]
    CauseMAR_att = file_info["CauseMAR_att"]
    stopEarlyAt= file_info['stop']
    # Initialize the estimators with the specific CauseMAR_att for each file
    EffiecientEstimator = Multi_UI.UI(isEfficienMARJoin=True, CauseMAR_att=CauseMAR_att,stopEarlyAt=stopEarlyAt)
    # Generate output filenames based on the input CSV filename
    timing_output_filename = f"{output_dir2}/{filename.split('/')[-1].replace('.csv', '_timing_results.txt')}"
    means_output_filename = f"{output_dir2}/{filename.split('/')[-1].replace('.csv', '_means_results.txt')}"

    # Using a single `with` block to handle both output files
    with open(timing_output_filename, "a") as timing_file, \
         open(means_output_filename, "a") as means_file:
        counter=0
        for query in queries:
            print("Done")
            # Calculate the true mean for the specific attribute in the query
# =============================================================================
#             attribute = query.split("AVG(")[1].split(")")[0]  # Extract the attribute name from the query
#             trueMean = trueData[attribute].mean()
#             print("true mean is ",trueMean )
# =============================================================================
            
            # Write the query and headers to the timing file
            timing_file.write(f"\n{query}\n")
            timing_file.write("efficientApproach_Time\n")
            
            # Write the query and headers to the means file
            #means_file.write(f"\n{query} | True mean is {trueMean:.4f}\n")
            means_file.write(f"\n{query} | \n")
            means_file.write("EfficientApproach_Mean\n")
            
            # Measure time and capture the estimated means for each approach
            print()
            print(f"Processing {filename} with query: {query}")
            print()
            print("--------Sample-Based Stop Early-----------------")
            start_time = time.time()
            #approach3_mean = EffiecientEstimator.run([query, "mar"])
            approach3_mean,_ = run_with_timeout(EffiecientEstimator, query)
            approach3_time = time.time() - start_time
            print("efficient time:"+str(approach3_time))
            print("efficient mean:"+str(approach3_mean))
            print()
            # If any of the estimators timed out, handle None results
            if approach3_mean == -11111111:
               # approach3_time = float('inf')  # Indicate timeout in time results
                print("Efficient approach timed out.")
            # Write timing results to the timing file
            timing_file.write(f"{approach3_time:.4f}\n")
            
            # Write the estimated means to the means file
            means_file.write(f"{approach3_mean:.4f}\n")