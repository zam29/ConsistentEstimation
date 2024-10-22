#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Sep 19 08:39:35 2023

for individual queries testing
"""
import time
import Multi_UI
import multiprocessing
import os
import re
# just to test single queries ... 

output_dir3 = "ModelBasedLog" # to record mdl time in the Multi_UI.py file 
if not os.path.exists(output_dir3):
    os.makedirs(output_dir3)
    
# # simple one relation model based
start_time = time.time()
multi_ui= Multi_UI.UI(isDrisbutionDriven=True)
x=multi_ui.run(["Select AVG(passenger_count) From nyc_subset10.csv where store_and_fwd_flag = 'Y'", "mcar"])
total = time.time() - start_time
print("total time: ", total)
 
# =============================================================================
# # simple one relation model based
# start_time = time.time()
# multi_ui= Multi_UI.UI(isDrisbutionDriven=True,CauseMAR_att=['A'])
# x=multi_ui.run(["Select AVG(B) From Table_T_w_Z2.csv where Z = 2", "mar"])
# total = time.time() - start_time
# print("total time: ", total)
#  
# 
# 
# # simple one relation sample based
# start_time = time.time()
# multi_ui= Multi_UI.UI(isEfficienMAR=True,CauseMAR_att=['A'])
# x=multi_ui.run(["Select AVG(B) From Table_T_w_Z2.csv where Z = 2", "mar"])
# total = time.time() - start_time
# print("total time: ", total)
# 
# 
# 
# # simple one relation sample based
# start_time = time.time()
# multi_ui= Multi_UI.UI(isEfficienMAR=True,stopEarlyAt=1, CauseMAR_att=['A'])
# x=multi_ui.run(["Select AVG(B) From Table_T_w_Z2.csv where Z = 2", "mar"])
# total = time.time() - start_time
# print("total time: ", total)
# =============================================================================


# =============================================================================
# # simple one relation sample based
# start_time = time.time()
# multi_ui= Multi_UI.UI(isEfficienMAR=True,CauseMAR_att=['Sex'])
# x=multi_ui.run(["Select AVG(HeightInMeters) From subset_haert_mar.csv", "mar"])
# total = time.time() - start_time
# print("total time: ", total)
# =============================================================================
