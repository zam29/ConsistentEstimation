#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""


Querying injected MCAR and MAR Data experiments for one relation and joins 
"""

import time
import Multi_UI
import multiprocessing
import os
import re
import  traceback


output_dir = "Injected/MAR"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)
    
output_dir2 = "Injected/MCAR"
if not os.path.exists(output_dir2):
    os.makedirs(output_dir2)
    
output_dir3 = "ModelBasedLog" # to record mdl time in the Multi_UI.py file 
if not os.path.exists(output_dir3):
    os.makedirs(output_dir3)
    


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
    
    return query
def run_estimator(estimator, query, return_dict,type_of_miss):

    try:
        # Run the estimator and store the result in the shared dictionary
        result = estimator.run([query, type_of_miss])

        # Check if the result is a tuple (for IntervalEstimator) or a single value (for others)
        if isinstance(result, tuple):
            return_dict['result'] = result  # For IntervalEstimator: handle as tuple (lower_mean, upper_mean)
        else:
            return_dict['result'] = (result, None)  # For single value estimators, use None for the second value
    except Exception as e:
        print(f"Error running estimator: {e}")
        traceback.print_exc() 
        return_dict['result'] = (-11111111, None) 

def run_with_timeout(estimator, query, type_of_miss, timeout_seconds=45):
    manager = multiprocessing.Manager()
    return_dict = manager.dict()  # Shared dictionary to store the result
    process = multiprocessing.Process(target=run_estimator, args=(estimator, query, return_dict,type_of_miss))

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



csv_queries = {
  "rwDatasets/bank_MAR.csv":
      {
        "queries": [
            "Select AVG(balance) From rwDatasets/bank_MAR_5.0.csv where education = primary",
            "Select AVG(balance) From rwDatasets/bank_MAR_5.0.csv",
            "Select AVG(balance) From rwDatasets/bank_MAR_5.0.csv where housing = no",
            "Select AVG(balance) From rwDatasets/bank_MAR_5.0.csv where loan = yes",
            "Select AVG(balance) From rwDatasets/bank_MAR_5.0.csv where duration < 100",
            
            
            "Select AVG(balance) From rwDatasets/bank_MAR_10.0.csv where education = primary",
            "Select AVG(balance) From rwDatasets/bank_MAR_10.0.csv",
            "Select AVG(balance) From rwDatasets/bank_MAR_10.0.csv where housing = no",
            "Select AVG(balance) From rwDatasets/bank_MAR_10.0.csv where loan = yes",
            "Select AVG(balance) From rwDatasets/bank_MAR_10.0.csv where duration < 100",
            
            
            "Select AVG(balance) From rwDatasets/bank_MAR_20.0.csv where education = primary",
            "Select AVG(balance) From rwDatasets/bank_MAR_20.0.csv",
            "Select AVG(balance) From rwDatasets/bank_MAR_20.0.csv where housing = no",
            "Select AVG(balance) From rwDatasets/bank_MAR_20.0.csv where loan = yes",
            "Select AVG(balance) From rwDatasets/bank_MAR_20.0.csv where duration < 100",
            
         
        ],
        
        "join": [

                "Select AVG(balance) from MarJoinsData/bank1_mar_joins1.csv join MarJoinsData/bank1_mar_joins1.csv, MarJoinsData/bank2_mar_joins2.csv on bank1_mar_joins1.csv.customer_id = bank2_mar_joins2.csv.customer_id",

         
        ],
        "CauseMAR_att": ['campaign']  # Specify CauseMAR_att for this dataset
    },
    
    "rwDatasets/nyc_MAR.csv": 
        {
    "queries": [
               "Select AVG(passenger_count) From rwDatasets/nyc_MAR_10.0.csv where trip_duration > 400 and store_and_fwd_flag = N",
                "Select AVG(passenger_count) From rwDatasets/nyc_MAR_5.0.csv",
                "Select AVG(passenger_count) From rwDatasets/nyc_MAR_5.0.csv where trip_duration > 400",
                "Select AVG(trip_duration) From rwDatasets/nyc_MAR_5.0.csv",
                "Select AVG(trip_duration) From rwDatasets/nyc_MAR_5.0.csv where store_and_fwd_flag = N",
                
                
                "Select AVG(passenger_count) From rwDatasets/nyc_MAR_10.0.csv where trip_duration > 400 and store_and_fwd_flag = N",
                "Select AVG(passenger_count) From rwDatasets/nyc_MAR_10.0.csv",
                "Select AVG(passenger_count) From rwDatasets/nyc_MAR_10.0.csv where trip_duration > 400",
                "Select AVG(trip_duration) From rwDatasets/nyc_MAR_10.0.csv",
                "Select AVG(trip_duration) From rwDatasets/nyc_MAR_10.0.csv where store_and_fwd_flag = N",
                
                
               "Select AVG(passenger_count) From rwDatasets/nyc_MAR_10.0.csv where trip_duration > 400 and store_and_fwd_flag = N",
                "Select AVG(passenger_count) From rwDatasets/nyc_MAR_20.0.csv",
                "Select AVG(passenger_count) From rwDatasets/nyc_MAR_20.0.csv where trip_duration > 400",
                "Select AVG(trip_duration) From rwDatasets/nyc_MAR_20.0.csv",
                "Select AVG(trip_duration) From rwDatasets/nyc_MAR_20.0.csv where store_and_fwd_flag = N",
        
     
    ],
    
    "join": [

            "Select AVG(passenger_count) from MarJoinsData/nyc1.csv join MarJoinsData/nyc1.csv, MarJoinsData/nyc2.csv on nyc1.csv.id = nyc2.csv.id",
            "Select AVG(trip_duration) from MarJoinsData/nyc1.csv join MarJoinsData/nyc1.csv, MarJoinsData/nyc2.csv on nyc1.csv.id = nyc2.csv.id",

        
     
    ],
    "CauseMAR_att": ['vendor_id']  # Specify CauseMAR_att for this dataset
},
        

            
    # Add more files and queries here

}
    
print("Querying MAR  ...")
"""
Querying MCAR data
"""

# Iterate over each file and its list of queries
for filename, file_info in csv_queries.items():
    queries = file_info["queries"]
    join_queries = file_info["join"]
    CauseMAR_att = file_info["CauseMAR_att"]
   

    
    # Initialize the estimators with the specific CauseMAR_att for each file

    EffiecientEstimator = Multi_UI.UI(isEfficienMAR=True, CauseMAR_att=CauseMAR_att)
    EffiecientJoinEstimator = Multi_UI.UI(isEfficienMARJoin=True, CauseMAR_att=CauseMAR_att)
    
    IntervalEstimator = Multi_UI.UI(isIntervalAnswer=True) 
    IntervalJoinEstimator = Multi_UI.UI(isIntervalJoin=True) 
    
    DistributionDrivenEstimator = Multi_UI.UI(isDrisbutionDriven=True,CauseMAR_att=CauseMAR_att)
    DistributionDrivenEstimator_join_MAR = Multi_UI.UI(isDistJoinQuery=True,CauseMAR_att=CauseMAR_att)
    # Generate output filenames based on the input CSV filename
    timing_output_filename = f"{output_dir}/{filename.split('/')[-1].replace('.csv', '_timing_results.txt')}"
    means_output_filename = f"{output_dir}/{filename.split('/')[-1].replace('.csv', '_means_results.txt')}"

    # Using a single `with` block to handle both output files
    with open(timing_output_filename, "w") as timing_file, \
         open(means_output_filename, "w") as means_file:
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
            timing_file.write("      ModelBased_Time    SampleBased Approach_Time     Interval_Est_Time\n")
            
            # Write the query and headers to the means file
            #means_file.write(f"\n{query} | True mean is {trueMean:.4f}\n")
            means_file.write(f"\n{query} | \n")
            means_file.write("   ModelBased_Mean   SampleBased Approach_Mean     Interval_Est_mean\n")
            
            # Measure time and capture the estimated means for each approach
            print()
            print(f"Processing {filename} with query: {query}")
            print()
            print("--------SampleBased  -----------------")
            start_time = time.time()
            #approach3_mean = EffiecientEstimator.run([query, "mar"])
            approach3_mean,_ = run_with_timeout(EffiecientEstimator, query,"mar")
            approach3_time = time.time() - start_time
            print("efficient time:"+str(approach3_time))
            print("efficient mean:"+str(approach3_mean))
            print()
            print("--------ModelBased  -----------------")
            queryQ=add_quotes_for_strings(query)
            start_time = time.time()
           # approach2_mean = DistributionDrivenEstimator.run([query, "mar"])
            approach2_mean,_ = run_with_timeout(DistributionDrivenEstimator,  queryQ,"mar")
            approach2_time = time.time() - start_time
            print("ModelBased mdl+exe time:"+str(approach2_time))
            print("ModelBased mean:"+str(approach2_mean))
            print("--------Interval_Est-----------------")
    
            start_time = time.time()
            #approach3_mean = EffiecientEstimator.run([query, "mar"])
            lower_mean, upper_mean = run_with_timeout(IntervalEstimator,  queryQ,"mar")
            approach4_time = time.time() - start_time
            if upper_mean is None:
                interval_estimate = f"[{lower_mean:.4f}, ?]"
            else:
                interval_estimate = f"[{lower_mean:.4f}, {upper_mean:.4f}]"
            print("Baseline time:"+str(approach4_time))
            print()
            # If any of the estimators timed out, handle None results
            if approach3_mean == -11111111:
               # approach3_time = float('inf')  # Indicate timeout in time results
                print("SampleBased  approach timed out.")
            

            # If any of the estimators timed out, handle None results
            if approach3_mean == -11111111:
               # approach3_time = float('inf')  # Indicate timeout in time results
                print("SampleBased  approach timed out.")
            
            if approach2_mean == -11111111:
                #approach2_time = float('inf')  # Indicate timeout in time results
                print("Distribution-driven approach timed out.")

        
            
            # Write timing results to the timing file
            timing_file.write(f"{approach2_time:.4f}\t\t\t\t\t\t{approach3_time:.4f}\t\t\t\t\t\t{approach4_time:.4f}\n")
            
            # Write the estimated means to the means file
            means_file.write(f"{approach2_mean:.4f}\t\t\t\t\t{approach3_mean:.4f}\t\t\t\t\t{interval_estimate}\n")
            
            
        for query in join_queries:
                print("Done")
                # Calculate the true mean for the specific attribute in the query
    # =============================================================================
    #             attribute = query.split("AVG(")[1].split(")")[0]  # Extract the attribute name from the query
    #             trueMean = trueData[attribute].mean()
    #             print("true mean is ",trueMean )
    # =============================================================================
                
            
                # Write the query and headers to the timing file
                timing_file.write(f"\n{query}\n")
                timing_file.write("      ModelBased_Time    SampleBased Approach_Time     Interval_Est_Time\n")
                
                # Write the query and headers to the means file
                #means_file.write(f"\n{query} | True mean is {trueMean:.4f}\n")
                means_file.write(f"\n{query} | \n")
                means_file.write("   ModelBased_Mean   SampleBased Approach_Mean     Interval_Est_mean\n")
                
                # Measure time and capture the estimated means for each approach
                print()
                print(f"Processing {filename} with query: {query}")
                print()
                print("--------SampleBased  Join-----------------")
                
                start_time = time.time()
                #approach3_mean = EffiecientEstimator.run([query, "mar"])
                approach3_mean,_ = run_with_timeout(EffiecientJoinEstimator, query,"mar")
                approach3_time = time.time() - start_time
                print("efficient time:"+str(approach3_time))
                print("efficient mean:"+str(approach3_mean))
                print()
                print("--------ModelBased -----------------")
                #queryQ=add_quotes_for_strings(query)
                start_time = time.time()
               # approach2_mean = DistributionDrivenEstimator.run([query, "mar"])
                approach2_mean,_ = run_with_timeout(DistributionDrivenEstimator_join_MAR,  query,"mar")
                approach2_time = time.time() - start_time
                print("ModelBased mdl+exe time:"+str(approach2_time))
                print("ModelBased mean:"+str(approach2_mean))
                print("--------Interval_Est-----------------")
                queryQ=add_quotes_for_strings(query)
                start_time = time.time()
                #approach3_mean = EffiecientEstimator.run([query, "mar"])
                lower_mean, upper_mean = run_with_timeout(IntervalJoinEstimator,  query,"mar")
                approach4_time = time.time() - start_time
                if upper_mean is None:
                    interval_estimate = f"[{lower_mean:.4f}, ?]"
                else:
                    interval_estimate = f"[{lower_mean:.4f}, {upper_mean:.4f}]"
                print("Baseline time:"+str(approach4_time))
                print()
                # If any of the estimators timed out, handle None results
                if approach3_mean == -11111111:
                   # approach3_time = float('inf')  # Indicate timeout in time results
                    print("SampleBased  approach timed out.")
                


            
                
                # Write timing results to the timing file
                timing_file.write(f"{approach2_time:.4f}\t\t\t\t\t\t{approach3_time:.4f}\t\t\t\t\t\t{approach4_time:.4f}\n")
                
                # Write the estimated means to the means file
                means_file.write(f"{approach2_mean:.4f}\t\t\t\t\t{approach3_mean:.4f}\t\t\t\t\t{interval_estimate}\n")





# Dictionary containing filenames and a list of queries for each file
csv_queries = {
  "rwDatasets/bank.csv":
      {
        "queries": [
            "Select AVG(balance) From rwDatasets/bank_MCAR_5.0.csv where education = primary",
            "Select AVG(balance) From rwDatasets/bank_MCAR_5.0.csv",
            "Select AVG(balance) From rwDatasets/bank_MCAR_5.0.csv where housing = no",
            "Select AVG(balance) From rwDatasets/bank_MCAR_5.0.csv where loan = yes",
            "Select AVG(balance) From rwDatasets/bank_MCAR_5.0.csv where duration < 100",
            
            
            "Select AVG(balance) From rwDatasets/bank_MCAR_10.0.csv where education = primary",
            "Select AVG(balance) From rwDatasets/bank_MCAR_10.0.csv",
            "Select AVG(balance) From rwDatasets/bank_MCAR_10.0.csv where housing = no",
            "Select AVG(balance) From rwDatasets/bank_MCAR_10.0.csv where loan = yes",
            "Select AVG(balance) From rwDatasets/bank_MCAR_10.0.csv where duration < 100",
            
            
            "Select AVG(balance) From rwDatasets/bank_MCAR_20.0.csv where education = primary",
            "Select AVG(balance) From rwDatasets/bank_MCAR_20.0.csv",
            "Select AVG(balance) From rwDatasets/bank_MCAR_20.0.csv where housing = no",
            "Select AVG(balance) From rwDatasets/bank_MCAR_20.0.csv where loan = yes",
            "Select AVG(balance) From rwDatasets/bank_MCAR_20.0.csv where duration < 100",

            
         
        ],
        "join": [

                "Select AVG(balance) from joinsData/bank1_mcar_joins1.csv join joinsData/bank1_mcar_joins1.csv, joinsData/bank2_mcar_joins2.csv on bank1_mcar_joins1.csv.customer_id = bank2_mcar_joins2.csv.customer_id",

        ],
       
    },
      
      "rwDatasets/nyc.csv":
          {
            "queries": [
                "Select AVG(passenger_count) From rwDatasets/nyc_MCAR_5.0.csv where store_and_fwd_flag = Y",
                "Select AVG(passenger_count) From rwDatasets/nyc_MCAR_5.0.csv",
                "Select AVG(passenger_count) From rwDatasets/nyc_MCAR_5.0.csv where trip_duration > 400",
                "Select AVG(trip_duration) From rwDatasets/nyc_MCAR_5.0.csv",
                "Select AVG(trip_duration) From rwDatasets/nyc_MCAR_5.0.csv where store_and_fwd_flag = N",
                
                
                "Select AVG(passenger_count) From rwDatasets/nyc_MCAR_10.0.csv where store_and_fwd_flag = Y",
                "Select AVG(passenger_count) From rwDatasets/nyc_MCAR_10.0.csv",
                "Select AVG(passenger_count) From rwDatasets/nyc_MCAR_10.0.csv where trip_duration > 400",
                "Select AVG(trip_duration) From rwDatasets/nyc_MCAR_10.0.csv",
                "Select AVG(trip_duration) From rwDatasets/nyc_MCAR_10.0.csv where store_and_fwd_flag = N",
                
                
                "Select AVG(passenger_count) From rwDatasets/nyc_MCAR_20.0.csv where store_and_fwd_flag = Y",
                "Select AVG(passenger_count) From rwDatasets/nyc_MCAR_20.0.csv",
                "Select AVG(passenger_count) From rwDatasets/nyc_MCAR_20.0.csv where trip_duration > 400",
                "Select AVG(trip_duration) From rwDatasets/nyc_MCAR_20.0.csv",
                "Select AVG(trip_duration) From rwDatasets/nyc_MCAR_20.0.csv where store_and_fwd_flag = N",

                
             
            ],
            "join": [

                "Select AVG(passenger_count) from joinsData/nyc1.csv join joinsData/nyc1.csv, joinsData/nyc2.csv on nyc1.csv.id = nyc2.csv.id",
                "Select AVG(trip_duration) from joinsData/nyc2.csv join joinsData/nyc2.csv, joinsData/nyc1.csv on nyc1.csv.id = nyc2.csv.id",

                
                
             
            ],
    },
          

    # Add more files and queries here
}

print("Querying MCAR ...")
"""
Querying MCAR data
"""

# Iterate over each file and its list of queries
for filename, file_info in csv_queries.items():
    queries = file_info["queries"]
    join_queries = file_info["join"]
  
   

    
    # Initialize the estimators with the specific CauseMAR_att for each file

    EffiecientEstimator_MCAR = Multi_UI.UI(stopEarly=False)
    EffiecientJoinEstimator_MCAR = Multi_UI.UI(isJoinQuery=True)
    
    DistributionDrivenEstimator_MCAR = Multi_UI.UI(isDrisbutionDriven=True)
    DistributionDrivenEstimator_MCAR_join =Multi_UI.UI(isDistJoinQuery=True)
    
    IntervalEstimator = Multi_UI.UI(isIntervalAnswer=True) 
    IntervalJoinEstimator = Multi_UI.UI(isIntervalJoin=True) 
    # Generate output filenames based on the input CSV filename
    timing_output_filename = f"{output_dir2}/{filename.split('/')[-1].replace('.csv', '_timing_results.txt')}"
    means_output_filename = f"{output_dir2}/{filename.split('/')[-1].replace('.csv', '_means_results.txt')}"

    # Using a single `with` block to handle both output files
    with open(timing_output_filename, "w") as timing_file, \
         open(means_output_filename, "w") as means_file:
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
            timing_file.write("      ModelBased_Time    SampleBased Approach_Time     Interval_Est_Time\n")
            
            # Write the query and headers to the means file
            #means_file.write(f"\n{query} | True mean is {trueMean:.4f}\n")
            means_file.write(f"\n{query} | \n")
            means_file.write("   ModelBased_Mean   SampleBased Approach_Mean     Interval_Est_mean\n")
            
            # Measure time and capture the estimated means for each approach
            print()
            print(f"Processing {filename} with query: {query}")
            print()
            print("--------SampleBased -----------------")
            start_time = time.time()
            #approach3_mean = EffiecientEstimator.run([query, "mar"])
            approach3_mean,_ = run_with_timeout(EffiecientEstimator_MCAR, query,"mcar")
            approach3_time = time.time() - start_time
            print("efficient time:"+str(approach3_time))
            print("efficient mean:"+str(approach3_mean))
            print()
            
            print("--------ModelBased  -----------------")
            queryQ=add_quotes_for_strings(query)
            start_time = time.time()
           # approach2_mean = DistributionDrivenEstimator.run([query, "mar"])
            approach2_mean,_ = run_with_timeout(DistributionDrivenEstimator_MCAR, queryQ,"mcar")
            approach2_time = time.time() - start_time
            print("ModelBased mdl+exe time:"+str(approach2_time))
            print("ModelBased mean:"+str(approach2_mean))
            print("--------Interval_Est-----------------")
            start_time = time.time()
            
            
            print("--------Interval_Est-----------------")
            queryQ=add_quotes_for_strings(query)
            start_time = time.time()
            #approach3_mean = EffiecientEstimator.run([query, "mar"])
            lower_mean, upper_mean = run_with_timeout(IntervalEstimator,  queryQ,"mcar")
            approach4_time = time.time() - start_time
            if upper_mean is None:
                interval_estimate = f"[{lower_mean:.4f}, ?]"
            else:
                interval_estimate = f"[{lower_mean:.4f}, {upper_mean:.4f}]"
            print("Baseline time:"+str(approach4_time))
            print()
            # If any of the estimators timed out, handle None results
            if approach3_mean == -11111111:
               # approach3_time = float('inf')  # Indicate timeout in time results
                print("SampleBased  approach timed out.")
            


        
            
            # Write timing results to the timing file
            timing_file.write(f"{approach2_time:.4f}\t\t\t\t\t\t{approach3_time:.4f}\t\t\t\t\t\t{approach4_time:.4f}\n")
            
            # Write the estimated means to the means file
            means_file.write(f"{approach2_mean:.4f}\t\t\t\t\t{approach3_mean:.4f}\t\t\t\t\t{interval_estimate}\n")
            
            
        for query in join_queries:
                print("Done")
                # Calculate the true mean for the specific attribute in the query
    # =============================================================================
    #             attribute = query.split("AVG(")[1].split(")")[0]  # Extract the attribute name from the query
    #             trueMean = trueData[attribute].mean()
    #             print("true mean is ",trueMean )
    # =============================================================================
                
            
                # Write the query and headers to the timing file
                timing_file.write(f"\n{query}\n")
                timing_file.write("      ModelBased_Time    SampleBased Approach_Time     Interval_Est_Time\n")
                
                # Write the query and headers to the means file
                #means_file.write(f"\n{query} | True mean is {trueMean:.4f}\n")
                means_file.write(f"\n{query} | \n")
                means_file.write("   ModelBased_Mean   SampleBased Approach_Mean     Interval_Est_mean\n")
                
                # Measure time and capture the estimated means for each approach
                print()
                print(f"Processing {filename} with query: {query}")
                print()
                print("--------SampleBased-----------------")
                
                start_time = time.time()
                #approach3_mean = EffiecientEstimator.run([query, "mar"])
                approach3_mean,_ = run_with_timeout(EffiecientJoinEstimator_MCAR, query,"mcar")
                approach3_time = time.time() - start_time
                print("efficient time:"+str(approach3_time))
                print("efficient mean:"+str(approach3_mean))
                print()
                
                
                print("--------ModelBased  -----------------")
                queryQ=add_quotes_for_strings(query)
                start_time = time.time()
               # approach2_mean = DistributionDrivenEstimator.run([query, "mar"])
                approach2_mean,_ = run_with_timeout(DistributionDrivenEstimator_MCAR_join, query,"mcar")
                approach2_time = time.time() - start_time
                print("ModelBased mdl+exe time:"+str(approach2_time))
                print("ModelBased mean:"+str(approach2_mean))
                print("--------Interval_Est-----------------")
                start_time = time.time()
                
                
                print("--------Interval_Est-----------------")
               
                start_time = time.time()
                #approach3_mean = EffiecientEstimator.run([query, "mar"])
                lower_mean, upper_mean = run_with_timeout(IntervalJoinEstimator,  query,"mcar")
                approach4_time = time.time() - start_time
                if upper_mean is None:
                    interval_estimate = f"[{lower_mean:.4f}, ?]"
                else:
                    interval_estimate = f"[{lower_mean:.4f}, {upper_mean:.4f}]"
                print("Baseline time:"+str(approach4_time))
                print()
                # If any of the estimators timed out, handle None results
                if approach3_mean == -11111111:
                   # approach3_time = float('inf')  # Indicate timeout in time results
                    print("SampleBased  approach timed out.")
                


            
                
                # Write timing results to the timing file
                timing_file.write(f"{approach2_time:.4f}\t\t\t\t\t\t{approach3_time:.4f}\t\t\t\t\t\t{approach4_time:.4f}\n")
                
                # Write the estimated means to the means file
                means_file.write(f"{approach2_mean:.4f}\t\t\t\t\t{approach3_mean:.4f}\t\t\t\t\t{interval_estimate}\n")