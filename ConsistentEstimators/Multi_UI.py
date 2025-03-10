#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Dec  7 08:42:22 2023
UI for the algorithms
it parses the input query and gets the attributes in the select, from, where, and join clauses 
it performs the modeling step for the model-based approach
it directs the chosen algorithms using the constructor flags
"""
import sampling
import generateDB
import csv
from Multi_GraphH import *
import aggregateFunctions
from efficient_MCAR import *
from efficient_MAR import *
import Selection_MAR
import IntervalAnswers
from DistributionDriven_Join import *
import time 
import os 
import threading
    
class UI:
    def __init__(self,isDrisbutionDriven=False,GetMARFullJointDist=False,isIntervalAnswer=False, 
                 isJoinQuery=False, isDistJoinQuery=False, isIntervalJoin=False,
                 isEfficienMAR=False, isEfficienMARJoin=False,stopEarly=True, isDrisbutionDriven_forFullMARJoin=False,
                 CauseMAR_att=[],CauseMAR_condition=[],stopEarlyAt=None):
        self.query = {}
        self.joinparams = []
        self.original_N=1
        self.isDrisbutionDriven=isDrisbutionDriven
        self.GetMARFullJointDist=GetMARFullJointDist
        self.isIntervalAnswer=isIntervalAnswer
        self.isJoinQuery=isJoinQuery
        self.isEfficienMAR=isEfficienMAR
        self.isDistJoinQuery=isDistJoinQuery
        self.CauseMAR_att=CauseMAR_att
        self.CauseMAR_condition=CauseMAR_condition
        self.stopEarly=stopEarly
        self.isEfficienMARJoin=isEfficienMARJoin
        self.isDrisbutionDriven_forFullMARJoin=isDrisbutionDriven_forFullMARJoin
        self.stopEarlyAt=stopEarlyAt
        self.isIntervalJoin=isIntervalJoin
       

    def displayCSV(self, filename):
        with open(filename, "r") as file:
            while file:
                print(next(file))
                
    def displayCSVSample(self, filename, start, end):
        i = 0;
        with open(filename, "r") as file:
            try:
                while file:
                    
                    if start == 0:
                        next(file)
                    line = next(file)
                    if i >= start and i < end:
                        print(line)
                    i += 1
            except StopIteration:
                pass
                
    def displayCSVHeader(self, filename):
        with open(filename, "r") as file:
            print(next(file))
            
    def querySetup(self, query): # boolean here 
        self.getQuery(query)
        start_time = time.time()
        if self.isDrisbutionDriven or self.isDistJoinQuery: 
           self.getJointProbTable() #27   - 70 70 
        self.graphH = []
        # make self.graphH an array, and iterate through it when setting up
        self.graphH.append(inputHandler(self.query['filenames']))
        if self.isDrisbutionDriven or self.isDistJoinQuery or self.isIntervalJoin or self.isIntervalAnswer or self.isDrisbutionDriven_forFullMARJoin : # or   self.isEfficienMAR :
            for graph in self.graphH:
                graph.fillFromCSV( self.isIntervalAnswer)
        end_time = time.time()
        print("cobstructed dist table time ", end_time - start_time)
        if self.isDrisbutionDriven or self.isDistJoinQuery:
            with open('ModelBasedLog/Modeling_and_exe_ModelBased_timing_log.txt', 'a') as f:
                f.write(f"Modeling step executed in {end_time - start_time:.4f} seconds. file = {self.query['filenames']}\n")
                f.write("================================================\n")

    def split_target(self,input_string):
        # Split each item on "(" and ")"
        # input example :['AVG(Age)']
        # output example : ['AVG', 'Age']
       result = []
       for item in input_string:
           result.extend([s.strip() for s in item.replace('(', ' ').replace(')', ' ').split() if s.strip()])
       return result 
    def is_3d(self,lst):
        return all(isinstance(i, list) for i in lst) and all(isinstance(j, list) for sublist in lst for j in sublist)
    
    def involvedTargetAttr(self, targetList): #[avg, age]
# input : [[], ['AVG', 'B'], ['AVG', 'C']] # output: [[], [ 'B'], [ 'C']]
       # get the list of the attributes involved in the query without aggration
       agg_lower = ["avg","sum","count"]
       to_return = []
       to_add_to_return = []
       for arr in targetList:
           for item in arr:
               if item.lower() not in agg_lower:
                   to_add_to_return.append(item)
           to_return.append(to_add_to_return.copy())
           to_add_to_return = []
       if self.is_3d(to_return):
                to_return= [item for sublist in to_return for subsublist in sublist for item in subsublist] 
       if len(to_return) == 1 and isinstance(to_return[0], list):
        # Convert [['B', 'D']] into [['B'], ['D']]
          to_return = [[item] for item in to_return[0]]
       return to_return

   
    def combinedSelectionCondtion(self,selectAttr,condtionAttr):
        # make them as a list [select,condtion]
        new_arr=[]
        for a in selectAttr :
            if a not in new_arr:
                new_arr.append(a)
        for i in range(0,len(condtionAttr)):
            temp = condtionAttr[i].split(" ")[0]
            if temp not in new_arr:
                new_arr.append(temp)
        return new_arr
    
    def involvedCondtionAttr(self, condtionAttr):
         condtionsAttributes = []
         for sublist in condtionAttr:
             temp_list = []
             for item in sublist:
                 # Split and get the first part of each condition
                 temp_list.append(item.split(" ")[0])
            # if temp_list:  # Ensure only non-empty sub-lists are appended
             condtionsAttributes.append(temp_list)
         return condtionsAttributes
    def getQuery(self, query):
        words = []
        words = query.split(" ")
        target = []
        conditions = []
        #joinFlag
        comparitors = ["<", ">", "<=", ">=", "="] #might need to add to this for things like !=
        for i in range(0, len(words)):
            for j in range(0, len(comparitors)):
                if comparitors[j] in words[i]:
                    if j == len(comparitors) - 1:
                        #looking at an =
                        words[i] = words[i].replace(comparitors[j]," == ")
                    else:
                        words[i] = words[i].replace(comparitors[j]," " + comparitors[j] + " ")
        for i in range(0, len(words)):
            words[i] = words[i].replace(",","")
        target, index = self.getSelectClause(words)
        target= self.split_target(target)
        filenames, index = self.getFromClause(words, index)

        target, conditions, selectCond_combined, index = self.getWhereClause(target, words, index)

        Joinfilenames, index ,joinCol1,joinCol2,predicates= self.getJoinClause(target, words, index)
        if Joinfilenames:
            filenames=Joinfilenames

        target= self.split_target(target)
        selectAttr = self.involvedTargetAttr(target)
        selectCond_combined = self.combinedSelectionCondtion(selectAttr, conditions)
        
        self.query={'filenames': filenames,'target':target,'conditions':conditions,'selectCond_combined':selectCond_combined,'joinCol1':joinCol1,'joinCol2':joinCol2,'predicates':predicates}

        
    def getSelectClause(self, words):
        index = 0
        target = []
        for i in range(1, len(words)):
            if words[i] == '*':
                target.append(self.graphH.headers)
            if words[i].lower() != "from":
                target.append(words[i])
            else:
                index = i
                break
        return target, index
    
    def getFromClause(self, words, index):
        index += 1
        next_keywords = ["where", "join", "group"]
        filenames = []
        while index!= len(words) and words[index].lower() not in next_keywords:
            filenames.append(words[index])
            index += 1
        return filenames, index
        
        
    def getJoinClause(self,target,words, index):
        #join_words = query.split("on")
        filenames =[]
        j1=''
        j2 = ''
        predct = ['']
        if  index == len(words) or words[index].lower() != "join":
            return filenames,index,j1,j2, predct
        filenames =[]
        index+=1
        while index!= len(words) and words[index] !='on':
            filenames.append(words[index])
            index+=1
        index+=1
        join_attrs= words[index:]
        comparitors = ["<", ">", "<=", ">=", "=="] #might need to add to this for things like !=
        join_attrs_wfiles=[]
        predicates=[]
        #i=0
        while index <len(words):
            while words[index].strip(" ") not in comparitors :
                join_attrs_wfiles.append(words[index])
                index+=1
            predicates.append(words[index].strip(" "))
            index+=1
            while  index <len(words) and words[index].strip(" ") not in comparitors :
                join_attrs_wfiles.append(words[index])
                index+=1
               
        joinCol1=join_attrs_wfiles[0].split(".")[-1]
        joinCol2=join_attrs_wfiles[1].split(".")[-1]
        return filenames, index, joinCol1,joinCol2, predicates

    def getWhereClause(self,target,words, index):
        conditions = []
        if index == len(words) or words[index].lower() != "where":
            #selectAttr = self.involvedTargetAttr(target) # Aug 17
            #selectCond_combined = self.combinedSelectionCondtion(selectAttr, conditions) # Aug 17
            #return target, [], selectCond_combined, index # Aug 17
            return target, [], [], index # Aug 17
        line = ""
        next_keywords = ["join", "group"]
        index += 1
        while index!= len(words) and words[index].lower() not in next_keywords:
            words[index] = words[index].replace(",","")
            if words[index] == "and":
                if line!="":
                 conditions.append(line)
                line = ""
            else:
                if line != "":
                    line += " "
                line += words[index]
            index += 1
        if line!="":
         conditions.append(line) #['C',']
        #split on () for cases like "AVG(AGE)" = > ["AVG","Age"]
        #print(target)
       # target= self.split_target(target)
       # print(conditions) 
        return target, conditions, [], index 
    
    def getJointProbTable(self):
        jbtFiles =[]
        jpt = sampling.JointProbability()
       
        for i in range(0,len(self.query['filenames'])):
           jointpt,N = jpt.compute_joint_probability(self.query['filenames'][i])
           jbtFiles.append(jointpt)
    
        self.query['filenames'] =jbtFiles
        self.original_N =N
        
    def run(self):
        query = ""
        query = input("Input Query Here:\n")
        self.querySetup(query)
        missingType = input("Input Missingness Type:\n")
        self.runQuery(missingType)
            
    def run(self, inputData):
        self.querySetup(inputData[0])
        if self.isIntervalAnswer:
             lb, ub = self.runQuery(inputData[1])
             return lb, ub
        else:
            answer =  self.runQuery(inputData[1])
            return answer
        

    def generateItertables(self, setInvolved): 
        
        listOfobsinTable =[]
        listOfHeadersInTable=[]
        for i in self.graphH:
          listOfobsinTable.append(i.observed) # lsit of obserevd attributes in csv file i 
        # merge results of every self.graphH.observed in the array of input Hnadlers that is in self.graphH
          listOfHeadersInTable.append(i.headers[:-1])  # lsit of headers in csv file i
        #itertables = [header for header, obsd in zip(listOfHeadersInTable, listOfobsinTable) #()
                      #if obsd and header not in setInvolved]
        itertables = []
        # Iterate over both lists
        for attr in setInvolved: 
            for i in range(len(listOfHeadersInTable)):
                iter_per_graphH=[]
                for j in range(len(listOfHeadersInTable[i])):
                    if listOfobsinTable[i][j] and listOfHeadersInTable[i][j] not in attr:
                        iter_per_graphH.append(listOfHeadersInTable[i][j])

                itertables.append(iter_per_graphH)
        
        return itertables
        
    
    def getItertableDomains(self, itertables): 
        
        allDomains =[]
        listOfHeadersInTable =[]
        for i in self.graphH :
                allDomains.append(i.getDomains()[:-1])
                listOfHeadersInTable.append(i.headers[:-1])
        domain = []
        for headers_row, domains_row in zip(listOfHeadersInTable, allDomains):
          domain_per_graphH = []
          for header, domains in zip(headers_row, domains_row):
              if domains and header not in [item for sublist in itertables for item in sublist]:
                  domain_per_graphH.append(header)
          if domain_per_graphH:  # Only append non-empty rows
             domain.append(domain_per_graphH)
        return domain
    def split_target_list_into_pairs(self, target_list):
        """
        input : ['A', 'E', 'W', 'AVG', 'B', 'AVG', 'C']   #[Avg, C]
        target_list : TYPE
            DESCRIPTION.

        Returns
        -------
        target[['A', 'E', 'W'], ['AVG', 'B'], ['AVG', 'C']]

        """
        agg_keywords =  ["AVG", "SUM", "COUNT"]
        # Find the index of the first occurrence of any aggregation keyword
        keyword_indexes = []
        for keyword in agg_keywords:
            for i in range(0, len(target_list)):
                if target_list[i] == keyword:
                    keyword_indexes.append(i)
                #min(target_list.index(keyword))
        # Split the list based on the index
        if len(keyword_indexes) == 0:
            return [target_list]
        first_part = target_list[:keyword_indexes[0]]
        remaining_part = target_list[keyword_indexes[0]:]
        # Group the remaining list in pairs
        remaining_pairs = [remaining_part[i:i+2] for i in range(0, len(remaining_part), 2)]
        # Combine the first part with the remaining pairs
        if first_part:
            target_list_of_list = [first_part] + remaining_pairs
        else:
            target_list_of_list =  remaining_pairs
        return target_list_of_list
        
    def runQuery(self, missingType):
        self.query['target']= self.split_target_list_into_pairs(self.query['target']) ## result in 2d list
        self.query['graphH'] = self.graphH
        selectAttr = self.involvedTargetAttr(self.query['target']) #[[Avg,B],[avg,C]] ->  # [[B],[C]]
        
        """ if it is one file, this means the slection part shares the same conditions"""
        """ The conditions will be a 2d list with one element, since we are iterating through selections,"""
        """ the constions must be duplicated in size of slecetions"""
        if len(self.graphH) == 1: # has one csv file in it
            self.query['conditions'] = [self.query['conditions'] for _ in range(len(selectAttr))]
        
        if missingType == "mcar":
            if   self.isDrisbutionDriven:
                for i in range(0,len(self.query['target'])): #[[], ['AVG', 'B'], ['AVG', 'C']]
                    test =aggregateFunctions.aggregateFunctions(self.original_N)
                    avg=test.mcarAVG(self.query['filenames'][0], self.query['target'][i], self.query['conditions'][i])
                    #print(" im here", avg)
                    return avg
            elif self.isIntervalAnswer:
                 for i in range(0,len(self.query['target'])): #[[], ['AVG', 'B'], ['AVG', 'C']]
                   intervalAggregator = IntervalAnswers.IntervalAnswers(self.query['filenames'][0], self.query['target'][i], self.query['conditions'][i],self.query['graphH'][0])
                   lowerbound, upperbound = intervalAggregator.getIntervalAnswer()
                   #print("Interval answer is [{}, {}]".format(lowerbound, upperbound))
                   return (lowerbound, upperbound)
               
            elif self.isIntervalJoin:
                 for i in range(0,len(self.query['target'])): #[[], ['AVG', 'B'], ['AVG', 'C']]
                   intervalAggregator = IntervalAnswers.IntervalAnswers(self.query['filenames'][0], self.query['target'][i], self.query['conditions'][i],self.query['graphH'][0])
                   lowerbound, upperbound = intervalAggregator.join_and_aggregate( self.query['filenames'][0],self.query['filenames'][1], self.query['joinCol1'], self.query['joinCol2'])
                   #print("Interval answer is [{}, {}]".format(lowerbound, upperbound))
                   return (lowerbound, upperbound)
               
            elif self.isJoinQuery: # effiecent join
                RJ= MCARRippleJoin(self.query['filenames'][0],self.query['filenames'][1], self.query['joinCol1'], self.query['joinCol2'], self.query['predicates'][0], self.query['target'][0])
                mean, ci=RJ.ripple_join(self.stopEarlyAt)
                #print("done effeceient")
                #print(mean,ci)
                return mean
            elif self.isDistJoinQuery:
                DiJ=DistributionDrivenJoin(self.query['filenames'][0],self.query['filenames'][1], self.query['joinCol1'], self.query['joinCol2'], self.query['predicates'][0], self.query['target'][0])
                # relation1File, relation2File, joinCol1, joinCol2, predicate, attr, aggFunc, conditions, TypeOfmissingness, originalN=1):
                mean = DiJ.ripple_join()
               # print("done dist")
                #print(mean)   
                return mean
            else: #effeicent one relation
                for i in range(0,len(self.query['target'])): 
                    Effiecient = oneRelationMCAR(self.query['filenames'][0],self.query['target'][i],self.query['conditions'][i],0.95)
                    mean, ci= Effiecient.aggregate(self.stopEarlyAt)
                   # print("efficient mean:"+str(mean))
                    return mean
                    
           
        elif missingType == "mar":
            if   self.isEfficienMAR:
                for i in range(0,len(self.query['target'])): 
                        effientMAR =EffecientMAROneRelation(self.query['filenames'][0],self.query['target'][i],[self.CauseMAR_att.copy()],self.query['conditions'][i])
                        weighted, ci =effientMAR.getExpectation(  self.stopEarlyAt, self.stopEarly)
                       # print(weighted)
                        #print(ci)
                        return weighted 
            elif self.isEfficienMARJoin:
                        effientMAR =MARRippleJoin(self.query['filenames'][0],self.query['filenames'][1], self.query['joinCol1'], self.query['joinCol2']
                                                      , self.query['predicates'][0], self.query['target'][0],[self.CauseMAR_att.copy()])
                       
                        weighted, ci =effientMAR.ripple_join(self.stopEarlyAt)
                        #print(weighted)
                        #print(ci)
                        return weighted 
            elif self.isDistJoinQuery: # dsiirbution driven join 
                DiJ=DistributionDrivenJoin(self.query['filenames'][0],self.query['filenames'][1], self.query['joinCol1'], self.query['joinCol2'], self.query['predicates'][0], self.query['target'][0],TypeOfmissingness="mar")
                mean = DiJ.ripple_join()  
               # print("done dist")
                #print(mean)   
                return mean
            
            elif self.isIntervalAnswer:
                 for i in range(0,len(self.query['target'])): #[[], ['AVG', 'B'], ['AVG', 'C']]
                   intervalAggregator = IntervalAnswers.IntervalAnswers(self.query['filenames'][0], self.query['target'][i], self.query['conditions'][i],self.query['graphH'][0])
                   lowerbound, upperbound = intervalAggregator.getIntervalAnswer()
                  # print("Interval answer is [{}, {}]".format(lowerbound, upperbound))
                   return (lowerbound, upperbound)
               
            elif self.isIntervalJoin:
                    for i in range(0,len(self.query['target'])): #[[], ['AVG', 'B'], ['AVG', 'C']]
                      intervalAggregator = IntervalAnswers.IntervalAnswers(self.query['filenames'][0], self.query['target'][i], self.query['conditions'][i],self.query['graphH'][0])
                      lowerbound, upperbound = intervalAggregator.join_and_aggregate( self.query['filenames'][0],self.query['filenames'][1], self.query['joinCol1'], self.query['joinCol2'])
                     # print("Interval answer is [{}, {}]".format(lowerbound, upperbound))
                      return (lowerbound, upperbound)
                  
            elif   self.isDrisbutionDriven or self.isDrisbutionDriven_forFullMARJoin: # one relation

                condAttr = self.involvedCondtionAttr(self.query['conditions'])
                #self.query.append(selectAttr) #query[3]
                self.query['selectAtt'] = selectAttr
                #self.query.append(condAttr) #query[4]
                self.query['condAttr'] = condAttr
                
                if not self.CauseMAR_att : #empty cause 
                    Numerator_iterables = self.generateItertables(self.query['selectAtt'])
                    Denominator_iterables = self.generateItertables(condAttr)
        
                    self.query['Numerator_iterables']=Numerator_iterables
                    self.query['Denominator_iterables']=Denominator_iterables

                    
                elif self.CauseMAR_condition and self.CauseMAR_condition != self.CauseMAR_att :
                    self.query['Numerator_iterables'] =[self.CauseMAR_att.copy().append(self.CauseMAR_condition.copy())]
                    self.query['Denominator_iterables']=  [self.CauseMAR_condition.copy()]
                else: 
                    self.query['Numerator_iterables'] =[self.CauseMAR_att.copy()]
                    self.query['Denominator_iterables']=  [self.CauseMAR_att.copy()]
               
                
                allDomains =[]
                allAttrs  =[]
                
                for i in self.graphH : # iterating over csv get domain for each csv
                    allDomains.append(i.getDomains()[:-1]) #[[domain for csv file1 ]]
                    allAttrs.append(i.getHeaders()[:-1])
                martest = Selection_MAR.queryGen(str(self.query['filenames'][0]),self.original_N,self.GetMARFullJointDist)
                
                for i in range(0,len(self.query['target'])): # over [[Avg,B],[avg,C]] 
                    martest.setQuery(allDomains[0], self.query['target'][i], self.query['conditions'][i], self.query['Numerator_iterables'][i], self.query['Denominator_iterables'][i], allAttrs[0], self.query['graphH'][0])
                    answer= martest.getSelection()
                    return answer
   

        
        

        
        
        
            