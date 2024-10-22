#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Sep 19 08:39:35 2023

Graph handler for model-based and interval-based
it gets the CSV data and fills them into an array-like data structure for easy access
it gets their active domains, and it has injected mcar missingness in it
"""

import generateDB
import csv
import random
import numpy as np


class Graph:
    def __init__(self, graphConfig):
        self.graphConfig = graphConfig
        self.connectGraph()
        self.getMaxSeperatingSet("R_age", "age")
    def connectGraph(self):
        temp_nodeName_arr = []
        for node in self.graphConfig.nodeArray:
            temp_nodeName_arr.append(node.nodeName)
        for node in self.graphConfig.nodeArray:
            for i in range(0, len(node.connections)):
                #print(temp_nodeName_arr.index(node.connections[i]))
                pass
    """         
    def getMinSeperatingSet(self, R_node, End_node, seperatingSet = []):
        #return anything that connects R_node to node, without a * in it
        if seperatingSet == []:
            nodeName_list = []
            for n in self.graphConfig.nodeArray:
                nodeName_list.append(n.nodeName)
            if R_node not in nodeName_list:
                print("ERROR: seperating set call has imaginary node")
                return -1
            if End_node not in nodeName_list:
                print("ERROR: seperating set call has imaginary node")
                return -1
        for node in self.graphConfig.nodeArray:
            if R_node in node.connections:
                seperatingSet.append(node.nodeName)
                return self.getSeperatingSet(R_node, node, seperatingSet)
            if End_node in node.connections:
                print(seperatingSet)
                return seperatingSet
        print("No Seperating Set found")
    """
    
    def getMaxSeperatingSet(self, R_node, End_node, seperatingSet = []):
        for node in self.graphConfig.nodeArray:
            if (node.isObserved) and ('*' not in node.nodeName):
                if (node.nodeName != R_node) and (node.nodeName != End_node):
                    seperatingSet.append(node.nodeName)
        return seperatingSet
    
class inputHandler:
    def __init__(self, input_filename = []):
        """
        example data:
        headers = ["Name", "Age", "income"]
        data = [['Bob', 45.0, 20000.0],
                'Timmy', 4.0, NaN]]
        observered = [True, True, False]
        
        parameters :
            self.graph :
            self.input_filename :
            self.setInputFile(input_filename) :
            self.data : arrray that stores the rows of the csv file
            self.headers : array that stores the set of attributes in the csv header
            self.observed : array of bools to line up with header array 
            to decide if the attribute is fully obsereved or partially obsereved 
            self.missingRep : array indicates the meiisngess repsentation that could appear in the data
            self.fileHandler: array that recives the csv file to handle the repesentation of data in the csv file
            self.dataTypes : an arry that stores the dtata types of the attributes
        """
        self.input_filename = input_filename
        self.data = []
        self.dataByTable = {}
        self.tableWiseHeaders = {} #self.tableWiseHeaders["X"] = ['a', 'b']
        self.tableWiseObserved = {}
        self.headers = [] #[Age, City, Name, ...]
        self.observed = [] #[1, 0, ]
        self.missingRep = ["", "NaN", "nan", None]
        self.fileHandler = []
        self.dataTypes = []
        self.setInputFiles()
        self.IntervalsDomains=[]
        #observed is array of bools to line up with header array
    
    def setInputFiles(self):
        """
        Sets the input file 
        """
        for file in self.input_filename:
            self.dataByTable[file] = []
            self.tableWiseHeaders[file] = []
            self.tableWiseObserved[file] = []
    def getHeaders(self):
        # rturns the list of attributes
        return self.headers
        
    def fillFromCSV(self, isIntervalAnswer=False):
        """
        stes the class memebers 

        Returns
        -------
        None.

        """
        for input_file in self.input_filename:
            with open(input_file, "r") as file:
                self.tableWiseHeaders[input_file] = (self.parseHeader(file))
                for i in range(0, len(self.tableWiseHeaders[input_file])):
                    self.tableWiseObserved[input_file].append(True)
                try:
                    while file:
                        row_input = self.parseRow(file)
                        # Check for missing values in row
                        for i in range(0, len(row_input)):
                            for option in self.missingRep:
                                if str(row_input[i]) == option:
                                    self.tableWiseObserved[input_file][i] = False
                                    
                        self.data.append(row_input)
                        self.dataByTable[input_file].append(row_input)
                except StopIteration:
                    pass
            for i in range(0, len(self.tableWiseHeaders[input_file])):
                self.headers.append(self.tableWiseHeaders[input_file][i])
                self.observed.append(self.tableWiseObserved[input_file][i])
# =============================================================================
#         if isIntervalAnswer:
#          self.IntervalsDomains= self.getDomains()
# =============================================================================
    
    def remove_element_2d(self, lst, element):
     for sub_lst in lst:
        if element in sub_lst:
            sub_lst.remove(element)
     return lst
    
    def getIntervalDomains(self):
        return self.IntervalsDomains
    
    def getDomains (self):
        mydata = np.array(self.data.copy())
       # mydata = self.data.copy()
        domains=[]
       # domains = [list(np.unique(mydata[:,i])) for i in range(mydata.shape[1]) ]
        col_num = mydata.shape
        if len(col_num) == 1:
            col_num = 1
        else:
            col_num = mydata.shape[1]
        for i in range(col_num):
            domains.append(list(np.unique(mydata[:,i]))) #[[1,2],[7,8]]
        domains= self.remove_element_2d(domains, 'NaN')
        return domains
    
    def parseHeader(self, file):
        """
        Parameters
        ----------
        file : csv file
        
        Returns
        -------
        header : set of attributes from the csv file

        """
        self.fileHandler = csv.reader(file)
        header = next(self.fileHandler)
        header = self.stripArrayNotation(header)      
        header = header.split(',')
        return header
           
    def parseRow(self, file):
        """

        Parameters
        ----------
        file : csv file
        Returns
        -------
        row : the tuple after being converted from string to proper data type

        """
        self.fileHandler = csv.reader(file)
        row = next(self.fileHandler)
        #row = self.stripArrayNotation(row)
        #row = self.convertDataToTuples(row)
        
        return row
    
           
    def stripArrayNotation(self, row):
        """

        Parameters
        ----------
        row : the row from csv that is represented as string ""

        Returns
        -------
        strToPrint : strips the array notation from the row

        """
        #strToPrint = list(str(self.colNames))
        strToPrint = list(str(row)) # [Age, hdhdd]
        #print("header before parse:", strToPrint)
        strToPrint.pop(0)
       # print("header after pop(0):", strToPrint)
        strToPrint.pop()
       # print("header after pop():", strToPrint)
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
       # print(strToPrint)
        return strToPrint
    
    def convertDataToTuples(self, row):
        """
        It would speed up the program if we could use some method to convert
        some parts of this to integers instead of just floats ands strings
        
        Parameters
        ----------
        row :  that is represented as string ""
        example : ["30.0, 3433.9, lubna", "60.0, 33.9, Huda"]

        Returns
        -------
        the tuple row with each element in the row being converted to its propoer type
        example : [(30.0, 3433.9, "lubna"), (60.0, 33.9, "Huda")]
        
        """
        parts = row.split(',')
        converted_parts = []
        for part in parts:
            try: 
                if part == 'NaN':
                   converted_part = part
                   #pass
                else:
                   converted_part = float(part)
            except ValueError:
                converted_part = part
            converted_parts.append(converted_part)
        
        return tuple(converted_parts)
    
    #def dataFloatsHandler(self):
        #inputData= self.data
        #self.dataAsTuples= [self.convertDataToTuples(row) for row in inputData]
        
       # return dataAsTuples
        

    def getObservedAttributes (self):
        """
        Returns
        -------
        The set of attribuets that are fullly obsereved 

        """
        ret_arr = []
        for i in range(0, len(self.headers)):
            if self.observed[i] == True:
                ret_arr.append(self.headers[i])
        return ret_arr
    def getPartiallyObservedAttributes(self):
        ret_arr = []
        for i in range(0, len(self.headers)):
            if self.observed[i] == False:
                ret_arr.append(self.headers[i])
        return ret_arr
    
    
class graphConfig:
    def __init__(self):
        self.nodeArray = []
    def addNode(self, nodeName, connections):
        new_node = node(nodeName, connections)
        self.nodeArray.append(new_node)
    def removeNode(self, nodeName):
        #self.nodeArray.remove(nodeName)
        pass
    
class node:
    def __init__(self, nodeName = "", nodeConnections = [], isObserved = True, missingness = 0.0):
        """

        Parameters
        ----------
        nodeName : Name of the current node
        nodeConnections : lsit of the node's neigbours 
        isObserved : whether the node is a fully obsereved attibute or not
        missingness : about of missingness - a number between 0 and 1

        Returns
        -------
        None.

        """
        self.nodeName = nodeName
        self.connections = nodeConnections
        self.isObserved = isObserved
        self.missingness = missingness
        


class MCAR(Graph):
    def __init__(self):
        pass
    def mcarHide(self, df, missingRate):
        """
        Parameters
        ----------
        df : the relation 
        missingRate : amount of missingness, number between 0 and 1

        Returns
        -------
        the relation with hidden values 
        """
        df['Age'] = df['Age'].sample(frac=missingRate)
        return df
    
class dataHandler:
    def __init__(self, input_file = "", output_file = ""):
        """
        input file:clean data file
        output_file: missing data file
        missingFunc: (code)Function used to hide values
        ...
        csvData: a inputHandler
        """
        """
        init of inputHandler
        self.graph = input_graph
        self.input_filename = ""
        self.setInputFile(input_filename)
        self.data = []
        self.headers = []
        self.observed = []
        self.missingRep = ["", "NaN", "nan", None]
        self.fileHandler = []
        self.dataTypes = []
        """
        
        self.input_file = input_file
        self.output_file = output_file
        self.missingFunc = None
        self.missingRate = None
        self.inputHandler = inputHandler(input_file)
        self.newInputHandler = inputHandler(input_file)
        self.inputScan()
        self.overallMissingRate=0
        self.target_missing_count = 0
        self.current_missing_count = 0


    def inputScan(self):
        self.inputHandler.fillFromCSV()
        
    def setParameters(self, missingRate, missingFunc):
        self.missingFunc = missingFunc
        self.missingRate = missingRate
        
    def setMissingNess(self):
        for i in range(0, len(self.inputHandler.data)):
            """
            WE SHOULD FIND A BETTER WAY TO WRITE THIS, BUT THE PRINCIPLE IS CORRECT
            """
            self.inputHandler.data[i] = list(self.inputHandler.data[i])
            self.inputHandler.data[i] = self.missingFunc(self.missingRate, self.inputHandler.data[i])
            self.inputHandler.data[i] = tuple(self.inputHandler.data[i])

            
    def outputToCSV(self):
        with open(self.output_file, "w") as file:
            writer = csv.writer(file)
            writer.writerow(self.inputHandler.headers)
            for i in range(0, len(self.inputHandler.data)):
                writer.writerow(self.inputHandler.data[i])
                
            
            
    def MCARmissing(self, missingRate, dataRow):
        #missingRate = [0, .5]
        # Inject missingness into the row if the target missing count is not yet reached
        for i in range(len(missingRate)):
            if self.current_missing_count >= self.target_missing_count:
                break
        
            if random.uniform(0, 1) < missingRate[i]:
                if dataRow[i] is not None:
                    dataRow[i] = None  # Inject missingness (None represents missing data)
                    self.current_missing_count += 1
# =============================================================================
#         for i in range(0, len(missingRate)-1):
#             if random.randint(0, 100) < missingRate[i] * 100:
#                 #hide the data
#                 dataRow[i] = None
# =============================================================================
                
        return dataRow
    
    def getMissingColsParams(self, overallMissingRate, numParents=1):
        self.overallMissingRate=overallMissingRate
        self.total_values = len(self.inputHandler.data) * len(self.inputHandler.headers)
        self.target_missing_count = int(self.overallMissingRate * self.total_values)
            
        numHeaders = len(self.inputHandler.headers)
        missingHeaders = numHeaders-numParents
        missing_col_rate = overallMissingRate * (numHeaders/missingHeaders)
        rates = []
        parentIndicies = []
        for i in range(0, numParents):
            parentIndicies.append(random.randint(0, numHeaders - 1))
        for i in range(0, numHeaders):
            if i in parentIndicies:
                rates.append(0)
            else:
                rates.append(missing_col_rate)
        return rates

    def is_numeric(self, value):
        try:
            float(value)
            return True
        except ValueError:
            return False


    
    def getObservedAttributes(self):
        return self.inputHandler.getObservedAttributes()
    
    def getPartiallyObservedAttributes(self):
        return self.inputHandler.getPartiallyObservedAttributes()
        
        
                
            
            
        
        

