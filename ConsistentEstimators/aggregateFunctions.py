#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MCAR aggregation functions executer 
"""
import pandas as pd
import csv
import numpy as np
import sys

sys.path.insert(1, "codeTests")
import codeAssembler

class aggregateFunctions : 
    def __init__(self,Original_N=1):
        
        self.codeGen = codeAssembler.codeGenerator(Original_N)
        self.keywords = ["AVG", "SUM", "COUNT"]
        pass
    def mcarAVG(self, jointProbablityTable, attributeOfInterest, conditions):
        """
        Parameters:
        - jointProbablityTable: A csv file that contains the joint probablities
        - attributeOfIntereset : string , the attribute that we want to compute the average for
        - condtions : a set of condtions on specified attributes  that has to be met to compute the avergae 
        pf the attributeOfInterest
        Returns:
        - The average of attributeOfInterest paired with a probablity 
        """
       # print(attributeOfInterest)
        for i in range(0, len(attributeOfInterest)-1):
            for word in self.keywords:
                if word.lower() == attributeOfInterest[i].lower() :
                    #this means a keyword was found, do the keyword operation to the next attribute
                    #e.g "AVG", "AGE"
                    # once you see "AVG", do "AVG"'s funciton on "AGE"
                    #if word.lower() == "avg":
                    if word.lower() == "avg":
                        aggfunction= word
                        attributeOfInterest.remove(word)
                        #print((attributeOfInterest))
                        self.codeGen.genAvgQuery(conditions, jointProbablityTable, attributeOfInterest, "pass")
                        avg=self.codeGen.runQuery(aggfunction)
                        return avg
                        break
                    elif  word.lower() == "sum":
                        aggfunction= word
                        attributeOfInterest.remove(word)
                        #change the function
                        self.codeGen.genSumQuery(conditions, jointProbablityTable, attributeOfInterest, "pass")
                        self.codeGen.runQuery(aggfunction)
                        break
                    elif word.lower() == "count":
                        aggfunction= word
                        attributeOfInterest.remove(word)
                        #change the function
                        self.codeGen.genCountQuery(conditions, jointProbablityTable, attributeOfInterest, "pass")
                        self.codeGen.runQuery(aggfunction)
                        break
                    else:
                        print("The "+ word +" query is not supported yet")
                        break

#test = aggregateFunctions()
#test.mcarAVG("row_counts.csv", ["Age"], ["Name == 'Alice'", "City == 'New York'", "Gender == 'Female'"])