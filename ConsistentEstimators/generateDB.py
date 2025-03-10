#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""


Generating datasets for debugging 
"""
import numpy as np
import pandas as pd
import random
#import matplotlib.pyplot as plt
import math

class dataGenerator:
    def __init__(self, colNames = [], rowNum = 0):
        """

        Parameters
        ----------
        colNames : attributes names
        rowNum : the row number

        Returns
        -------
        None.

        """
        self.colNames = colNames
        self.rowNum = rowNum
        self.numCols = len(colNames)
        self.curRows = 0
        self.data = []
        
    def generateRow(self, distFunc):
        """

        Parameters
        ----------
        distFunc : TYPE
            DESCRIPTION.

        Returns
        -------
        None.

        """
        row = []
        for col in range(0, self.numCols):
            row.append(distFunc[col])
        self.data.append(row)
    
    def hideCol(self, dist):
        pass
        
    def R2dDist(self, V_min, V_max):
        """
        Parameters
        ----------
        V_min : minimum value that can be assigned to the cell
        V_max : maximum value that can be assigned to the cell

        Returns
        -------
         a random value to the cell

        """
        A = random.randint(V_min, V_max)
        B = random.randint(V_min, V_max)
        return (A + B) / 2
    
    def augmentRelation(self, df):
        """

        Parameters
        ----------
        df : a complete datatframe 

        Returns
        -------
        df : dataframe with MCAR msiingess

        """
        df['R_age'] = df['Age'].apply(lambda x: 1 if math.isnan(x) else 0)
        df.to_csv('augData.csv', index=False)  
        return df
        
        
   # def hideRandom(self, df, missingRate):
        #df['Age'] = df['Age'].sample(frac=missingRate)
        #return df
        
    def generateTable(self,mini,maxi):
        """

        Returns
        -------
        randomly generated table

        """
        for i in range(0, self.rowNum):
            self.generateRow([self.R2dDist(mini, maxi), self.R2dDist(mini, maxi)])
            
    def printToCSV(self, filename):
        """

        Parameters
        ----------
        filename : name of csv file that will be generated

        Returns
        -------
        None.

        """
        if not open(filename, "w"):
            print("ERROR: file could not be created")
        self.printHeader(filename)
        for i in range(0, self.rowNum):
            self.printRow(i, filename)
            
    def printRow(self, index, filename):
        """

        Parameters
        ----------
        index : index of the row to be written to.
        filename : csv file name

        Returns
        -------
        None.

        """
        with open(filename, "a") as file:
            strToPrint = list(str(self.data[index])) # original without id column
           # strToPrint = list(str([index + 1] + self.data[index])) # for id column
            strToPrint.pop(0)
            strToPrint.pop()
            for i in range(0, len(strToPrint) - 1):
                if strToPrint[i] == ",":
                    strToPrint.pop(i+1)
            strToPrint="".join(strToPrint)
            file.write(strToPrint)
            file.write("\n")
        pass
    
    def printHeader(self, filename):
        """
        prints attributes names to csv file
        Parameters
        ----------
        filename :  csv file name

        Returns
        -------
        None.

        """
        with open(filename, "a") as file:
            strToPrint = list(str(self.colNames)) # original without id column
           # strToPrint = list(str(["ID"] + self.colNames)) # for id column
            strToPrint.pop(0)
            strToPrint.pop()
            strToPrint.pop(0)
            strToPrint.pop()
            counter = len(strToPrint) -1
            i=0
           # for i in range(1, len(strToPrint) - 1):
            while i < counter:
                if strToPrint[i] == ",":
                    strToPrint.pop(i+1)
                    strToPrint.pop(i+1)
                    strToPrint.pop(i-1)
                    counter -= 3
                i += 1
            strToPrint="".join(strToPrint)
            file.write(strToPrint)
            file.write("\n")
        
        
        """
        # Generate random data for age and salary
        data = {'Age': [random.randint(18, 65) for _ in range(N)],
            'Salary': [random.randint(30000, 100000) for _ in range(N)]}
        # Create a DataFrame from the data
        df = pd.DataFrame(data)
        # Save the DataFrame to a CSV file
        df.to_csv('cleanData.csv', index=False)  
        # 'data.csv' is the file name, and index=False avoids writing row numbers as an additional column
        
       
        #print("the dirty table is")
       # print(df)
        #jontdf =pd.crosstab(df["Salary"], df["Age"],margins=True, normalize=True) # joint table
        #print("=============Joint table is=================")
        #print(jonttest)
        return df
        """


#-------------------------------------------------------------
"""
missR= 1-.3  # 30% missingness 
#N=[10,100,1000,10000,100000]
N=[10]
for n in N:
   relation= generateTable (n)
print(relation)
relation = hideRandom(relation,missR)
print(relation)
relation= augmentRelation(relation)
print(relation)
"""
