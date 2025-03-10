#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""


the interval-based algorithm as our baseline
we extended it to cover join, just a simple extension

"""

import pandas as pd
import numpy as np

class IntervalAnswers():
    def __init__(self, relationFile, attr, predicates,graphHandler=None):
        self.relationFile = relationFile
        self.attribute = attr[1]  # The attribute to aggregate (column B)
        self.aggregation_func = attr[0]  # The aggregation function (AVG, SUM, COUNT)
        self.graphHandler=graphHandler
        """For one predicate version"""
       # self.predicate_str = predicates[0]  # Predicate string, e.g., "A > 4"
        """Multiple predicate handling"""
        self.predicate_str = " & ".join(predicates) if predicates else None 
    def read_csv(self, file_path):
        return pd.read_csv(file_path)
    
    def convertDoainToNumbers(self,input_list, drop_strings=["NA",'',np.nan]):
        result = []
        for i in range(0, len(input_list)):
            if isinstance(input_list[i], list):
                #we are in a 2D array
                result.append([])
                for j in range(0, len(input_list[i])):
                    flag = 0
                    for k in range(0, len(drop_strings)):
                        if input_list[i][j] == drop_strings[k]:
                            flag = 1
                    if flag == 0:
                        #not a drop string
                        try:
                            result[i].append(float(input_list[i][j]))
                        except TypeError:
                            result[i].append(input_list[i][j])
            else:
                #for 1D array
                flag = 0
                for k in range(0, len(drop_strings)):
                    if input_list[i] == drop_strings[k]:
                        flag = 1
                if flag == 0:
                    #not a drop string
                    try:
                        result.append(float(input_list[i]))
                    except TypeError:
                        result.append(input_list[i])
        return result
                        
    def minMaxValsOfAggAttribute(self):
        indexOfAggAttribute = self.graphHandler.headers.index(self.attribute)
        #Alldomains = self.graphHandler.getDomains()
        Alldomains = self.graphHandler.getIntervalDomains()
        domainOfAggAttribute = Alldomains[indexOfAggAttribute]
        domainOfAggAttribute = self.convertDoainToNumbers(domainOfAggAttribute)
        b= max([x for x in domainOfAggAttribute ])
        a= min([x for x in domainOfAggAttribute ])
        #print("i am here")
        return a, b
    
    def directMinMAx (self,):
        completeFilename =self.relationFile
        if "hidden" in completeFilename:
           completeFilename = completeFilename.replace("hidden", "complete")
        CompleteR= self.read_csv(completeFilename )
        return CompleteR[self.attribute].agg(['min', 'max'])
        
    def getIntervalAnswer(self):
        #self.minMaxValsOfAggAttribute()
        a,b = self.directMinMAx()
       # a, b = self.minMaxValsOfAggAttribute()
       # a,b =1,5
        if self.aggregation_func.lower() == 'avg':
            return self.query_evaluation_avg(a, b)
        elif self.aggregation_func.lower() == 'count' or self.aggregation_func.lower() == 'sum':
            return self.query_evaluation_sum_count(a, b)
        else:
            raise ValueError("Aggregation function not supported")
        
    def query_evaluation_sum_count(self, a, b):
        # Load the dataset
        R = self.read_csv(self.relationFile)
        

        
        if self.predicate_str:  # If there are predicates
            TA = R.query(self.predicate_str)
            relevant_columns = [pred.split()[0] for pred in self.predicate_str.split(" & ")]
            # MA should include rows where any relevant column is null
            MA = R[R[relevant_columns].isna().any(axis=1)]
        else:  # No predicates: select all rows for TA and handle MA based on missing aggregation attribute
            TA = R
            MA = R[R[self.attribute].isna()]
        
       # print(MA)
        if self.aggregation_func.lower() == "count":
            lb = len(TA)
            ub = len(TA) + len(MA)
        elif self.aggregation_func.lower() == "sum":
            TAc = TA[self.attribute].dropna()
            TA_null = TA[self.attribute][TA[self.attribute].isna()]
            Ac = pd.concat([TAc, MA[self.attribute].dropna()])
            A_null = pd.concat([TA_null, MA[self.attribute][MA[self.attribute].isna()]])
    
            lb = TAc.sum() + a * len(TA_null)
            ub = Ac.sum() + b * len(A_null)
        else:
            raise ValueError("Invalid aggregate function. Use either 'SUM' or 'COUNT'.")
    
        return lb, ub
    
    def avg(self, values):
        return sum(values) / len(values) if values else 0

    
    def query_evaluation_avg(self, a, b):
        # Load the dataset
        R = self.read_csv(self.relationFile)
        

        if self.predicate_str:  # If there are predicates
            TA = R.query(self.predicate_str)
            relevant_columns = [pred.split()[0] for pred in self.predicate_str.split(" & ")]
            # MA should include rows where any relevant column is null
            MA = R[R[relevant_columns].isna().any(axis=1)]
        else:  # No predicates: select all rows for TA and handle MA based on missing aggregation attribute
            TA = R
            MA = R[R[self.attribute].isna()]

        TA_upper = TA[self.attribute].fillna(b)
        MA_upper = MA[self.attribute].fillna(b)
    
        MA_sorted = MA_upper.sort_values(ascending=False)
        
        S1 = []
        for value in MA_sorted:
            if self.avg(TA_upper.tolist() + S1) < value:
                S1.append(value)
            else:
                break
        ub = self.avg(TA_upper.tolist() + S1)
    
        # Substitute nulls in TA and MA with a for lower bound
        TA_lower = TA[self.attribute].fillna(a)
        MA_lower = MA[self.attribute].fillna(a)
    
        MA_sorted = MA_lower.sort_values(ascending=True)
        S2 = []
        for value in MA_sorted:
            if self.avg(TA_lower.tolist() + S2) > value:
                S2.append(value)
            else:
                break
        lb = self.avg(TA_lower.tolist() + S2)
    
        return lb, ub
    
    def join_on_interval(self, tuple1, tuple2, joinCol1,joinCol2,join_col1_lb, join_col1_ub,join_col2_lb,  join_col2_ub):
        """
        Joins two tuples if their values are within the same interval.
        """
        value1 = tuple1[joinCol1]
        value2 = tuple2[joinCol2]
        if value1 == value2 :
            return {**tuple1, **tuple2}
        elif join_col2_lb <= value1 <= join_col2_ub or join_col1_lb <= value2 <= join_col1_ub: # "or" cuz one of them maybe nan
            #print("falls within:", join_col2_lb,  join_col2_ub )
           # print("falls within:", join_col1_lb,  join_col1_ub )
            return {**tuple1, **tuple2}  # Join the tuples by merging dictionaries
        return None
    
    def join_and_aggregate(self, file1, file2, joinCol1,joinCol2):
        """
        Joins tuples from two files if they are in the same interval and returns aggregation of self.attribute.
        """

        df1 = self.read_csv(file1)
        join_col1_lb, join_col1_ub =df1[joinCol1].agg(['min', 'max'])
        
        df2 = self.read_csv(file2)
        join_col2_lb, join_col2_ub =df2[joinCol2].agg(['min', 'max'])
         
        joined_relation = []

        #  join based on the interval condition
        for _, tuple1 in df1.iterrows():
            for _, tuple2 in df2.iterrows():
                joined_tuple = self.join_on_interval(tuple1, tuple2, joinCol1,joinCol2,join_col1_lb, join_col1_ub,join_col2_lb,  join_col2_ub)
                if joined_tuple:
                    joined_relation.append(joined_tuple)

        # Convert joined relation into a DataFrame for aggregation
        if joined_relation:
            joined_df = pd.DataFrame(joined_relation)
           # print(joined_df)
            # Save the joined relation to a CSV file and call get IntervalAnswer forone relation
            joined_df.to_csv("Interval_joined_relation.csv", index=False)
            self.relationFile = "Interval_joined_relation.csv"
            return self.getIntervalAnswer()
        else:
            return None, None  # If no tuples were joined




