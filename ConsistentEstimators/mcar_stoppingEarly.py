#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Oct 10 16:56:05 2024
Stopping early for single relation and relation joins 
MCAR case
change the percentage of rows to process

"""



import time
import Multi_UI
import multiprocessing
import os
import re 
percentage =0.2
#output_dir = "Mcar_results_OneR_injected/Bitcoin"
output_dir = "Mcar_StoppingEarly/OneR_1_percent"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

output_dir2 = "Mcar_StoppingEarly/Join_1_percent"
if not os.path.exists(output_dir2):
    os.makedirs(output_dir2)

    
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
        result = estimator.run([query, "mcar"])

        # Check if the result is a tuple (for IntervalEstimator) or a single value (for others)
        if isinstance(result, tuple):
            return_dict['result'] = result  # For IntervalEstimator: handle as tuple (lower_mean, upper_mean)
        else:
            return_dict['result'] = (result, None)  # For single value estimators, use None for the second value
    except Exception as e:
        print(f"Error running estimator: {e}")
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

#mcar injection example
# =============================================================================
# datasetsnames =['BitcoinHeistData_complete.csv']
# missngness_percentages = [0.05,0.10,0.20]
# for miss in missngness_percentages:
#     
#     for i in  datasetsnames :
#         print("injecting Bitcoin MCAR in complete Relation : "+i)
#         hiddenname = i.replace("complete","MCAR_"+str(10*miss))
#         DataHandler = dataHandler( ["rwDatasets/"+i], "rwDatasets/"+hiddenname)
#         MissinRate= DataHandler.getMissingColsParams(miss,0)
#         DataHandler.setParameters(MissinRate, DataHandler.MCARmissing)
#         DataHandler.setMissingNess()
#         DataHandler.outputToCSV()
# =============================================================================



# Dictionary containing filenames and a list of queries for each file
csv_queries = {

      "rwDatasets/Street.csv":
          {
            "queries": [
                "Select AVG(PermitTotalSqFeet) From rwDatasets/Street_Construction_Permits_26.csv where BoroughName = BROOKLYN",
                "Select AVG(PermitLinearFeet) From rwDatasets/Street_Construction_Permits_26.csv where PermitNumberOfZones = 1 and PermitTotalSqFeet = 1",
                "Select AVG(PermitEstimatedNumberOfCuts) From rwDatasets/Street_Construction_Permits_26.csv",

             
            ],
            "stop":(2420000 * percentage)
        },
    "rwDatasets/nyc.csv":
        {
          "queries": [
                "Select AVG(passenger_count) From rwDatasets/nyc_MCAR_10.0.csv where trip_duration > 400 and store_and_fwd_flag = N",
                "Select AVG(trip_duration) From rwDatasets/nyc_MCAR_10.0.csv",
                "Select AVG(trip_duration) From rwDatasets/nyc_MCAR_10.0.csv where store_and_fwd_flag = N",

              
           
          ],
          "stop":(729000 * percentage)
         
      },
     "rwDatasets/building.csv":
         {
           "queries": [
               "Select AVG(TOTAL_FEE) From rwDatasets/Building_Permits_complete.csv where PERMIT_STATUS = OPEN",
               "Select AVG(BUILDING_FEE_PAID) From rwDatasets/Building_Permits_complete.csv where WORK_DESCRIPTION = MAINTENANCE",
               "Select AVG(REPORTED_COST) From rwDatasets/Building_Permits_complete.csv",
 
            
           ],
           "stop":(780000 * percentage)
       },
    # Add more files and queries here
}


print("Querying MCAR data ...")
"""
Querying MCAR data
"""

# Iterate over each file and its list of queries
for filename, file_info in csv_queries.items():
    queries = file_info["queries"]
    stopEarlyAt= file_info['stop']

    
    # Initialize the estimators with the specific CauseMAR_att for each file

    #EffiecientEstimator = Multi_UI.UI(isJoinQuery=True)
    EffiecientEstimator = Multi_UI.UI(stopEarlyAt=stopEarlyAt,stopEarly=False)
  
   

    # Generate output filenames based on the input CSV filename
    timing_output_filename = f"{output_dir}/{filename.split('/')[-1].replace('.csv', '1percent_timing_results.txt')}"
    means_output_filename = f"{output_dir}/{filename.split('/')[-1].replace('.csv', '1percent_means_results.txt')}"

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
            timing_file.write("efficientApproach_Time\n")
            
            # Write the query and headers to the means file
            #means_file.write(f"\n{query} | True mean is {trueMean:.4f}\n")
            means_file.write(f"\n{query} | \n")
            means_file.write("EfficientApproach_Mean\n")
            
            # Measure time and capture the estimated means for each approach
            print()
            print(f"Processing {filename} with query: {query}")
            print()
            print("--------Efficient -----------------")
            start_time = time.time()
            #approach3_mean = EffiecientEstimator.run([query, "mar"])
            approach3_mean,_ = run_with_timeout(EffiecientEstimator, query)
            approach3_time = time.time() - start_time
            print("effecient time:"+str(approach3_time))
            print("effecient mean:"+str(approach3_mean))
            print()


            # Write timing results to the timing file
            timing_file.write(f"{approach3_time:.4f}\n")
            
            # Write the estimated means to the means file
            means_file.write(f"{approach3_mean:.4f}\n")

print("-----------------------------------------------------------JOIN QUERIES ----------------------------------------------------")
# Dictionary containing filenames and a list of queries for each file
csv_queries = {

      "rwDatasets/Street.csv":
          {
            "queries": [

                "Select AVG(PermitNumberOfZones) from joinsData/street1_join.csv join joinsData/street1_join.csv, joinsData/street3_join.csv on street3_join.csv.PermitNumber = street1_join.csv.PermitNumber",
                "Select AVG(PermitTotalSqFeet) from joinsData/street1_join.csv join joinsData/street1_join.csv, joinsData/street3_join.csv on street3_join.csv.PermitNumber = street1_join.csv.PermitNumber",

                
                
             
            ],
           "stop":(2420000 *2* percentage)
        },
    "rwDatasets/nyc.csv":
        {
          "queries": [

              "Select AVG(passenger_count) from joinsData/nyc1.csv join joinsData/nyc1.csv, joinsData/nyc2.csv on nyc1.csv.id = nyc2.csv.id",
              "Select AVG(trip_duration) from joinsData/nyc2.csv join joinsData/nyc2.csv, joinsData/nyc1.csv on nyc1.csv.id = nyc2.csv.id",

              
              
           
          ],
          "stop":(729000 *2* percentage)
         
      },
     "rwDatasets/building.csv":
         {
           "queries": [

               "Select AVG(BUILDING_FEE_SUBTOTAL) from joinsData/building2.csv join joinsData/building2.csv, joinsData/building1.csv on building1.csv.customer_id = building2.csv.customer_id",
               "Select AVG(ZONING_FEE_SUBTOTAL) from joinsData/building2.csv join joinsData/building2.csv, joinsData/building1.csv on building1.csv.customer_id = building2.csv.customer_id",

               
               
            
           ],
          "stop":(780000 *2* percentage)
       },
    # Add more files and queries here
}


print("Querying MCAR data ...")
"""
Querying MCAR data
"""

# Iterate over each file and its list of queries
for filename, file_info in csv_queries.items():
    queries = file_info["queries"]
    stopEarlyAt= file_info['stop']
    
    # Initialize the estimators with the specific CauseMAR_att for each file

    EffiecientEstimator = Multi_UI.UI(stopEarlyAt=stopEarlyAt,isJoinQuery=True)
    #EffiecientEstimator = Multi_UI.UI(stopEarlyAt=stopEarlyAt,stopEarly=False)
  
   

    # Generate output filenames based on the input CSV filename
    timing_output_filename = f"{output_dir2}/{filename.split('/')[-1].replace('.csv', '10percent_timing_results.txt')}"
    means_output_filename = f"{output_dir2}/{filename.split('/')[-1].replace('.csv', '10percent_means_results.txt')}"

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
            timing_file.write("efficientApproach_Time\n")
            
            # Write the query and headers to the means file
            #means_file.write(f"\n{query} | True mean is {trueMean:.4f}\n")
            means_file.write(f"\n{query} | \n")
            means_file.write("EfficientApproach_Mean\n")
            
            # Measure time and capture the estimated means for each approach
            print()
            print(f"Processing {filename} with query: {query}")
            print()
            print("--------Efficient -----------------")
            start_time = time.time()
            #approach3_mean = EffiecientEstimator.run([query, "mar"])
            approach3_mean,_ = run_with_timeout(EffiecientEstimator, query)
            approach3_time = time.time() - start_time
            print("effecient time:"+str(approach3_time))
            print("effecient mean:"+str(approach3_mean))
            print()


            # Write timing results to the timing file
            timing_file.write(f"{approach3_time:.4f}\n")
            
            # Write the estimated means to the means file
            means_file.write(f"{approach3_mean:.4f}\n")