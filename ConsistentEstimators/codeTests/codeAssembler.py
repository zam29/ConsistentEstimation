#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Oct 20 17:46:01 2023

A code generator class to generate model-based MCAR algorithms 
Given that input queries differ among users,
the algorithms based on a distribution-driven approach will also vary. 
To address this, We developed a code generator that dynamically produces code on the fly, matching the user's input and the corresponding algorithm.
"""

import pandas as pd
import csv
import numpy as np
import os
import time



#creating the for loop for iterationuery
class codeGenerator:
    def __init__(self, Original_N=1):
        self.Original_N=Original_N
        pass
    
    def strWithDelim(self, str_arr, delim):
        ret_str = ""
        for i in range(0, len(str_arr)):
            ret_str += str_arr[i] + delim
        return ret_str
    
    def condtionLine(self, condtions):
        result = "row['"
        for i in range(len(condtions)):
           #result += f"{condtions[i][0]} {condtions[i][1]} {condtions[i][2]} " 
           result += f"{condtions[i][0]}"+"'] "
           result += f"{condtions[i][1]} {condtions[i][2]} " 
           if i < len(condtions) - 1:
             result += "and row['"

        result += ":\n"
        return result
    
    def setupAvgQuery(self):
        
        
        pass
    def runQuery(self,Aggfunction):
        if Aggfunction.lower() == "avg":
            print("running the injected code for average")
            start_time = time.time()
            with open("codeTests/inject_code.py","r") as rnf:
              exec(rnf.read())
              end_time = time.time()
              print("time spent: ",end_time - start_time)
              with open('ModelBasedLog/Modeling_and_exe_ModelBased_timing_log.txt', 'a') as f:
                 f.write(f"MCAR query executed in {end_time - start_time:.4f} seconds. \n")
                 f.write("================================================\n")

              calculated_avg = globals().get('avginject', None)  # Fetches `avg` from the global namespace
              return calculated_avg
        if Aggfunction.lower() == "sum":
            print("running the injected code for sum")
            with open("codeTests/inject_code_sum.py","r") as rnf:
             exec(rnf.read())
        if Aggfunction.lower() == "count":
            print("running the injected code for count")
            with open("codeTests/inject_code_count.py","r") as rnf:
             exec(rnf.read())
# =============================================================================
#     def runQuerySum(self):
#             print("running the injected code")
#             #import inject_code
#            # os.system("codeTests/inject_code.py")
#             with open("codeTests/inject_code_sum.py","r") as rnf:
#              exec(rnf.read())
# =============================================================================
    
    def genAvgQuery(self, input_condition, joint_prob_table, 
                    attr_of_interest, exec_function):
        #input_condition = ["Name == 'Alice'", "City == 'New York'"]
        #attr_of_interest = ["Age"]
        #exec_function = "print(i2, i3)"
        #filenames,
        #import setup
        with open("codeTests/inject_code.py", "w") as code_file:
            setup_str = "df2 = pd.read_csv('" + joint_prob_table + "')\n#df2=df2.dropna()\navg =0\npRz =0\n"
            import_str = "import pandas as pd\nimport csv\nimport numpy as np\n\n" 
            
            
            
            name_of_probability = "P" #TO-DO make last col name
            input_cond_str = ""
            headers = []
            condtions = []
           # if len(input_condition) != 0:
            for i in range(0 , len(input_condition)):
                if input_cond_str != "":
                    input_cond_str += "and "
                input_condition_list = input_condition[i].split(' ', 2)
                condtions.append(input_condition_list)
                headers.append(input_condition_list[0])
                for j in range(0, 3):
                    input_cond_str += input_condition_list[j] + " "
            
           
            exec_function = "\t\t" + exec_function +"\n" #needs testing

           # headers = []
            if headers == []:
                for_iter = "for index, row in df2.iterrows():\n"
               # for_iter += "\tif not (row.isnull().any()):\n"
                for_iter += "\tif not pd.isna(row['"+self.strWithDelim(attr_of_interest, "")+"']):\n" #trying to check just for att of intereset
                for_iter += "\t\tpRz += row['P']\n"
                for_iter += "##print(pRz)\n"
                #for_iter += "if pRz==0: pRz=1\n"
                
                for_iter_2 = "for "
                for_iter_2 += self.strWithDelim(attr_of_interest, ", ")
                for_iter_2 += "P in zip(df2['"
                for_iter_2 += self.strWithDelim(attr_of_interest, "'],df2['")
                for_iter_2 += "P']):\n"
                
                for_iter_3 = "\t#print( "
                for_iter_3 += self.strWithDelim(attr_of_interest, ", ")
                for_iter_3 += "P)\n"
                for_iter_3 += "\tif not pd.isna(" +  attr_of_interest[0]+"):\n"
                for_iter_3 += "\t\tfrac = (P/pRz) * " + attr_of_interest[0] + "\n"
                for_iter_3 += "\t\tavg += frac\n"
                for_iter_3 += "print(\"avg is \"+str(avg))\n"
                for_iter_3 += "globals()['avginject'] = avg\n"
                for_iter_arr = [for_iter, for_iter_2, for_iter_3]
                to_print = [import_str, setup_str] + for_iter_arr
                code_file.writelines(to_print)
            else:
                for_iter = "for index, row in df2.iterrows():\n"
                #for_iter += "\t\tif not (row.isnull().any()) and "
                for_iter += "\tif not pd.isna(row['"+self.strWithDelim(attr_of_interest, "")+"']) and " #trying to check just for att of intereset
                for_iter += self.condtionLine(condtions)
                for_iter += "\t\t\tpRz += row['P']\n"
                for_iter += "print(pRz)\n"
                for_iter_2 =""
                for_loop_str = "for "
                return_cond = len(headers) + len(attr_of_interest) + 1
                iz=[]
                for i in range(0, return_cond):
                    if i < len(headers):
                        for_loop_str += headers[i]
                    elif i >= len(headers) and i < return_cond - 1:
                        for_loop_str += attr_of_interest[i - len(headers)]
                        iz.append(attr_of_interest[i - len(headers)])
                    else:
                        for_loop_str += 'P'
                        iz.append('P')
                    if i != return_cond - 1:
                        for_loop_str += ", "
                for_loop_str += " in zip(df2['"
                for_loop_str += self.strWithDelim(headers, "'],df2['")
                for_loop_str += self.strWithDelim(attr_of_interest, "'],df2['")
                for_loop_str += name_of_probability + "']):\n"
               #  if_string = "\tif " + input_cond_str + ":\n"
                if_string = "\tif not pd.isna(" +  attr_of_interest[0]+") and " + input_cond_str + ":\n"
                for_iter_3 = for_loop_str +if_string
                #for_iter_3 +=  exec_function
                for_iter_3 += "\t\tfrac = ("+iz[0]+"/pRz) * " + iz[1] + "\n"
                for_iter_3 += "\t\tavg += frac\n"
                for_iter_3 += "print(\"avg is \"+str(avg))\n"
                for_iter_3 += "globals()['avginject'] = avg\n"
                for_iter_arr = [for_iter, for_iter_2, for_iter_3]
                to_print = [import_str, setup_str] + for_iter_arr
                code_file.writelines(to_print)
    def genSumQuery(self, input_condition, joint_prob_table, 
                        attr_of_interest, exec_function):
            #input_condition = ["Name == 'Alice'", "City == 'New York'"]
            #attr_of_interest = ["Age"]
            #exec_function = "print(i2, i3)"
            #filenames,
            #import setup
            with open("codeTests/inject_code_sum.py", "w") as code_file:
                setup_str = "df2 = pd.read_csv('" + joint_prob_table + "')\ndf2=df2.dropna()\nsum_var =0\n"
                import_str = "import pandas as pd\nimport csv\nimport numpy as np\n\nN="+str(self.Original_N)+"\n"  
                
                
                
                name_of_probability = "P" #TO-DO make last col name
                input_cond_str = ""
                headers = []
                condtions = []
               # if len(input_condition) != 0:
                for i in range(0 , len(input_condition)):
                    if input_cond_str != "":
                        input_cond_str += "and "
                    input_condition_list = input_condition[i].split(' ', 2)
                    condtions.append(input_condition_list)
                    headers.append(input_condition_list[0])
                    for j in range(0, 3):
                        input_cond_str += input_condition_list[j] + " "
                
               
                exec_function = "\t\t" + exec_function +"\n" #needs testing

               # headers = []
                if headers == []:
                    for_iter = "for index, row in df2.iterrows():\n"
                    for_iter += "\tif not (row.isnull().any()):\n"
                    for_iter += "\t\tsum_var += (row['P'] * row['" + attr_of_interest[0] + "'])\n"
                    for_iter += "print(N*sum_var)\n"
                    
                
                    for_iter_arr = [for_iter]
                    to_print = [import_str, setup_str] + for_iter_arr
                    code_file.writelines(to_print)
                else:
                    for_iter = "for index, row in df2.iterrows():\n"
                    for_iter += "\t\tif not (row.isnull().any()) and "
                    for_iter += self.condtionLine(condtions)
                    for_iter += "\t\tsum_var += (row['P'] * row['" + attr_of_interest[0] + "'])\n"
                    for_iter += "#print(N*sum_var)\n"
                    for_iter_arr = [for_iter]
                    to_print = [import_str, setup_str] + for_iter_arr
                    code_file.writelines(to_print)
    def genCountQuery(self, input_condition, joint_prob_table, 
                        attr_of_interest, exec_function):
            #input_condition = ["Name == 'Alice'", "City == 'New York'"]
            #attr_of_interest = ["Age"]
            #exec_function = "print(i2, i3)"
            #filenames,
            #import setup
            with open("codeTests/inject_code_count.py", "w") as code_file:
                setup_str = "df2 = pd.read_csv('" + joint_prob_table + "')\ndf2=df2.dropna()\nsum_var =0\n"
                import_str = "import pandas as pd\nimport csv\nimport numpy as np\n\nN="+str(self.Original_N)+"\n" 
                
                
                
                name_of_probability = "P" #TO-DO make last col name
                input_cond_str = ""
                headers = []
                condtions = []
               # if len(input_condition) != 0:
                for i in range(0 , len(input_condition)):
                    if input_cond_str != "":
                        input_cond_str += "and "
                    input_condition_list = input_condition[i].split(' ', 2)
                    condtions.append(input_condition_list)
                    headers.append(input_condition_list[0])
                    for j in range(0, 3):
                        input_cond_str += input_condition_list[j] + " "
                
               
                exec_function = "\t\t" + exec_function +"\n" #needs testing

               # headers = []
                if headers == []:
                    for_iter = "for index, row in df2.iterrows():\n"
                    for_iter += "\tif not (row.isnull().any()):\n"
                    for_iter += "\t\tsum_var += (row['P'])\n"
                    for_iter += "print(N*sum_var)\n"
                    
                
                    for_iter_arr = [for_iter]
                    to_print = [import_str, setup_str] + for_iter_arr
                    code_file.writelines(to_print)
                else:
                    for_iter = "for index, row in df2.iterrows():\n"
                    for_iter += "\t\tif not (row.isnull().any()) and "
                    for_iter += self.condtionLine(condtions)
                    for_iter += "\t\tsum_var += (row['P'] )\n"
                    for_iter += "#print(N*sum_var)\n"
                    for_iter_arr = [for_iter]
                    to_print = [import_str, setup_str] + for_iter_arr
                    code_file.writelines(to_print)
    


