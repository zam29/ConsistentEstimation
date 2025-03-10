#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 21 13:09:04 2024
-Selection, projection, and full joint distribution for MAR
-MAR aggregation functions

TODO: some factors overlap; we can record them and reuse them again
"""

import pandas as pd
import csv
import numpy as np
import duckdb
import itertools
import StateMachine
import os
import time

class queryGen():
    def __init__(self, csv_filename,Original_N=1,MARFullJointDist=False):
        self.joint_csv = csv_filename
        self.Original_N=Original_N
        self.MARFullJointDist=MARFullJointDist
        
   # def setQuery(self, Domains, SelectionDomains, Selections, Conditions, Numerator_iterables, Denominator_iterables,graphH):
    def setQuery(self, Domains, Selections, Conditions, Numerator_iterables, Denominator_iterables,selectCond_combined, graphH):
        self.Domains = Domains
      #  self.SelectionDomains = SelectionDomains
        self.Selections = Selections
        self.Conditions=Conditions
        self.Numerator_iterables=Numerator_iterables
        self.Denominator_iterables=Denominator_iterables
        self.graphH = graphH
        self.nonobs = []
        self.obs = []
        # for boston datasest:
       # self.Numerator_iterables=['rm']
       # self.Denominator_iterables=['rm']
        self.epsilon = 1e-10
        
        obs_indicator = self.graphH.observed.copy()[:-1] #[T,F,T] #P
        for i in range(0,len(obs_indicator)):
            if  obs_indicator[i] == False :
             self.nonobs.append(self.graphH.headers[i])
            else:
             self.obs.append(self.graphH.headers[i])
             
        self.selectCond_combined=selectCond_combined
        self.deno_nonobs= [elem for elem in self.nonobs if elem not in self.Selections]
        
        """for MAR joint probablity purposes"""
        self.twod_list=[]

    def convert_to_numbers(self, input_list, drop_strings=['',np.nan]):
        """
        Parameters
        ----------
        lists : 2d list string with ' '
           # input : [['', '0.0', '1.0','xyz'], [ '0.0', '1.0']]
           # output : [[ 0.0, 1.0,'xyz''], [ 0.0', 1.0]]
        drop_strings : TYPE, optional
            DESCRIPTION. The default is [''].

        Returns
        -------
        list
            numbers

        """

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
                        except ValueError:
                            result[i].append(input_list[i][j])
            else:
                #for 1D array
                flag = 0
                for k in range(0, len(drop_strings)):
                    #if input_list[i][j] == drop_strings[k]:
                    if input_list[i] == drop_strings[k]:
                        flag = 1
                if flag == 0:
                    #not a drop string
                    try:
                        result.append(float(input_list[i]))
                    except ValueError:
                        result.append(input_list[i])
        return result
                
    def makeCondition(self, sumOver,location,  iterOverValue = []):
        conditions = ""
        if location=='nom':
           self.twod_list.append(sumOver+iterOverValue)
       # print(domvals)
        if location == 'dom':
          iterOverName= self.get_iter_for_MakeCondtion() 
          iterOverName=[] #Boston 
          #new
          if len(iterOverName) == 0:
            iterOverName = self.Denominator_iterables
              
        else :
              iterOverName= self.Selections
        
        k = 0
        for j in sumOver : 
                if conditions != "":
                    conditions += " and " 
                conditional_val = str(j)
                if not conditional_val.replace('.', '').isdigit():
                    qoute="'"
                    conditional_val= qoute+conditional_val+"'"
                conditions += self.Denominator_iterables[k] + " == " + conditional_val
               # conditions += self.Denominator_iterables[k] + " == " + str(j)
                k += 1
        if iterOverName != self.Denominator_iterables: #Boston
            for j in range(0, len(iterOverName)) : 
                    if conditions != "":
                        conditions += " and " 
                    conditional_val = str(iterOverValue[j])
                    if not conditional_val.replace('.', '').isdigit():
                        qoute="'"
                        conditional_val= qoute+conditional_val+"'"
                    conditions += iterOverName[j] + " == " + conditional_val    
        for k in range(0, len(self.Conditions)): # ['C==1','E==1']
            if not (self.Conditions[k] == ""): 
                conditions += " and " + self.Conditions[k]
       # print(conditions)
        return conditions
    
    def getItertablesAndThierValues(self,domains,iteratables): 
        """

        Parameters
        ----------
        domains : list of domains after the state machine.
        iteratables : list of attributes to iterate over 

        Returns
        -------
        params : dictional of all possible combinations
        
        Example : domains= [0,1], iteratables = ["B", "D"]
        output : 
            {'B': 0, 'D': 0}
            {'B': 0, 'D': 1}
            {'B': 1, 'D': 0}
            {'B': 1, 'D': 1}

        """
        for p in itertools.product(*[domains]*len(iteratables)):
            params = dict(zip(iteratables, p))
        return params
      
    def reduceQuery(self, query,location):
        """
        Parameters
        ----------
        
        query : String that represent the condtion
        location: nom or deno
        
        if there is a nonobserved attribute in the query replave it with .notna()
        
        the selections are [B,D] B is unobserevd
        the condtions are [C==1, E==1] C is unobserevd 
        
        from graph hander we have a lost of unobserved att in the table : [B,C]
        selecton non ob = [B]
        condtion non ob = [C]
        
        start by reduing the unobserevd variables in the selection
        then reduing the unboserevd variables in the condtions
        
        example : first call 
                  input : A == 0 and D == 0 and B == 0 and C==1 and E==1
                  output : A == 0 and D == 0 and B.notna() and C==1 and E==1 , True
                  
                  second call :
                     input : A == 0 and D == 0 and B.notna() and C==1 and E==1
                     output : A == 0 and D == 0 and C==1 and E==1 , True
                     
                thrid call : 
                    input : A == 0 and D == 0 and C==1 and E==1
                    output : A == 0 and D == 0 and C.notna() and E==1, True
                    
                fourth call :
                    input : A == 0 and D == 0 and C.notna() and E==1
                    output : A == 0 and D == 0 and E==1, True
                    
                fifth call : 
                   input  A == 0 and D == 0 and E==1  // all obserevd 
                   output :  A == 0 and D == 0 and E==1, False
                    

        Returns
        -------
       the updated query and a boolean 
        """
        non_obs_arr= self.nonobs[:]

            
        newQuery =""
        flag = 0
        queryInput=query[:]
        active = True
        dot ="."

        compare = ["<", ">", "<=", ">=", "=="]
        #assume that nonobs is in the order of [selections, conditions], and order outside of that does not matter
        querySplit = query.split(" ")
        for i in range(0, len(non_obs_arr)): 
            """
            instead 454 : nonobsVar= self.nonobs[0] ?
            """
            for j in range(0, len(querySplit)):
              if non_obs_arr[i] == querySplit[j] or  querySplit[j] == ""+non_obs_arr[i]+".notna()":
                #select self.nonobs[i]
               # for i in range(0,len(querySplit)) #B
                if j == (len(querySplit)-1) or querySplit[j+1] =="and" : # or querySplit[j] == ""+self.nonobs[i]+".notna()": # july 31
                #if querySplit[j+1] =="and" or querySplit[j] == ""+self.nonobs[i]+".notna()": #new
                   query=query.replace(" and "+non_obs_arr[i]+".notna()", "")
                   #print(query)
                   flag = 1
                   break
                   
                else :
                    if querySplit[j+1] =="==" :
                       query=query.replace(non_obs_arr[i]+" == "+ querySplit[j+2], non_obs_arr[i]+".notna()")
                       #query=query.replace(self.nonobs[i]+"=="+ querySplit[j+2], self.nonobs[i]+".notna()")
                       flag = 1
                       break

            if flag:
                break
                
        newQuery= query[:]
        
        if newQuery == queryInput:
            active= False
        return newQuery, active
    def get_corresponding_domains(self,domains, all_att, targetAtt):
        """
        Parameters
        ----------
        domains : 2d list of all domains of all attributes 
        all_att : list of all attributes
        targetAtt : 2d lsit of attribues thst we want their domains

        Returns
        -------
        target_domains : 2d lsit of attribues domains
        
        example : 
            domains = [['0.0', '1.0'], ['', '0.0', '3.0'], ['', '0.0', '1.0'], ['0.0', '1.0'], ['0.0', '1.0']],
            targetAtt= ["B", "D"],  
            all_att=  ["A","B","C","D","E"] 
            
            output : [['', '0.0', '3.0'],['0.0', '1.0']]

        """
        target_domains = []
        for att, domain in zip(all_att, domains):
            if att in targetAtt:
                target_domains.append(domain)
        return target_domains
    
    def remove_elements(self,list1):
        """
        retuns the the iterate over for the denominator
        Parameters
        ----------
        list1 : list of attributes

        Returns
        -------
        list of attributes withouth the unobserebed ones

        """
        returned_list = [elem for elem in list1 if elem not in self.nonobs] #[] 
        # if list is empty 
        if len(returned_list) != 0:
    
            atts_in_selection= self.Selections.copy() # B
            all_atts = self.selectCond_combined.copy() 
            filter_set = set(atts_in_selection) 
            full_condtions = self.Conditions.copy()
           # if full_condtions[0] != '':
            if len(full_condtions)!=0 and full_condtions[0] != '': # it was the '' alone
               atts_in_condtion = [condition.split()[0] for condition in full_condtions]
               return [x for x in all_atts if x not in filter_set  and x not in atts_in_condtion and x not in self.Denominator_iterables and x not in self.Numerator_iterables]
            else :
                return [x for x in all_atts if x not in filter_set and x not in self.Denominator_iterables and x not in self.Numerator_iterables]
        else:
            return returned_list
            
            
    def splitCondtion(self,  )    :
        words = []
        words = self.Conditions.copy() # ['c==1', '3==1']
        target = []
        comparitors = ["<", ">", "<=", ">=", "=="] #might need to add to this for things like !=
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
        index = 0
        for i in range(1, len(words)):
            if words[i].lower() != "from":
                target.append(words[i])
            else:
                index = i
                break
        conditions = []
        for i in range(0, len(words)):
            conditions.append(words[i].split("  "))
        return conditions
    
    def get_iter_for_MakeCondtion(self):
        #gets the itertable list 
        allAtt = self.selectCond_combined.copy()
        Selection= self.Selections.copy()
        nonobs= self.nonobs.copy()
        full_condtions = self.Conditions.copy()
       # atts_in_condtion =[]
        #if full_condtions[0] != '':
        if len(full_condtions)!=0 and full_condtions[0] != '':
          atts_in_condtion = [condition.split()[0] for condition in full_condtions]
        else :
            atts_in_condtion = []
        for attr in Selection: #B
            if attr in allAtt and attr in nonobs: #B,C
                allAtt.remove(attr) # remove B [A,C,D,E]

        allAtt= [elem for elem in allAtt if elem not in self.Numerator_iterables and elem not in self.Denominator_iterables  and elem not in atts_in_condtion] #[C,E]
        allAtt= [elem for elem in allAtt if elem not in self.Numerator_iterables and elem not in self.Denominator_iterables  and elem not in atts_in_condtion and elem not in nonobs] #[C,E]
     
        return allAtt
                
    def Avg_summingOver(self, Y_domain_list, selection_output_list):
        split_size = len(selection_output_list) // len(Y_domain_list)
        
        # Split f into chunks based on the number of elements in Y
        splits = [selection_output_list[i * split_size: (i + 1) * split_size] for i in range(len(Y_domain_list))]
        #print("splits:",splits)
        # Calculate the sum of each split
        sums = [sum(split) for split in splits]
       # print("sum of each split:",sums)
        # Multiply each element in Y with its corresponding list's sum
        result = [Y_domain_list[i] * sums[i] for i in range(len(Y_domain_list))]
        
        return sum(result)
    def marAVG(self, ) :
        Selection_query_list = self.getSelection()
        Y_domain = self.get_corresponding_domains(self.Domains,self.selectCond_combined,self.Selections)
        Y_domain  = sum(Y_domain, [])
        avg=self.Avg_summingOver(Y_domain,Selection_query_list)
       # for y , fraction in zip(Y_domain,Selection_query_list):
         #   avg+= y * fraction
        print("mar avg is:",avg)
        return avg
        
        
    def marSum(self, ) :
        Selection_query_list = self.getSelection()
        Y_domain = self.get_corresponding_domains(self.Domains,self.selectCond_combined,self.Selections)
        Y_domain  = sum(Y_domain, [])
        mar_sum=self.Avg_summingOver(Y_domain,Selection_query_list) * self.original_N
        print("mar sum is:",mar_sum)
        return mar_sum
    
    def marCount(self, ) :
        Selection_query_list = self.getSelection()
        mar_count=sum(Selection_query_list) * self.original_N
        print("mar count is:",mar_count)
        return mar_count
        
    def getSelection (self):
        #run
       # word = "AVG"
        start_time = time.time()
        self.keywords = ["AVG", "SUM", "COUNT"]
        for word in self.keywords:
            if word.lower() == self.Selections[0].lower() and word.lower() == "avg":
                self.Selections.remove(word)
                return self.marAVG()
            
            elif word.lower() == self.Selections[0].lower() and word.lower() == "sum":
                self.Selections.remove(word)
                return self.marSum()
            
            elif word.lower() == self.Selections[0].lower() and word.lower() == "count":
                self.Selections.remove(word)
                return self.marCount()
            
            else:
                values2 =[]
                numVals =self.SelectQueryMAR_Nom('nom')
                answers=[]
                dom = self.SelectQueryMAR_Nom('dom')
                dom+=self.epsilon
                for i in numVals:
                    answers.append((i/dom))
                if self.MARFullJointDist:
                    self.get_Full_MAR_JointDist(answers)
                end_time = time.time()
                with open('ModelBasedLog/Modeling_and_exe_ModelBased_timing_log.txt', 'a') as f:
                   f.write(f"MAR query executed in {end_time - start_time:.4f} seconds. \n")
                   f.write("================================================\n")
                return answers
    def get_Full_MAR_JointDist(self,answers):
            for idx, val in enumerate(answers):
                self.twod_list[idx].append(val)
            column_names = self.Numerator_iterables+self.Selections +['P']
            self.twod_list.insert(0, column_names)
            data=self.twod_list.copy()
            # Ensure domains_list is correctly processed into sub-lists of appropriate length 
            joint_MAR_df = pd.DataFrame(data[1:], columns=data[0])
            directory, filename = os.path.split(self.joint_csv)
            new_filename = "MAR_FullJoint_" + filename
            output_csv = os.path.join(directory, new_filename)
            joint_MAR_df.to_csv(output_csv, index=False)
   
    def extract_numbers_from_list(self, lst):
        numbers = []
        i = 0
        for i in range(0, len(lst)):
            for j in range(0, len(lst[i])):
                try:
                    numbers.append(float(lst[i][j]))
                except AttributeError:
                    pass
                except ValueError:
                    pass
        return numbers
    
    def SelectQueryMAR_Nom (self,location): 
        df = pd.read_csv(self.joint_csv)
     # converting the domains into numbers
        self.Domains = self.convert_to_numbers(self.Domains)
        stateMachine = StateMachine.StateMachine()
        itertateOver = []
        if location == 'nom': # for numernator
           num_iter_dom = self.get_corresponding_domains(self.Domains,self.selectCond_combined,self.Numerator_iterables)
           sumOver= stateMachine.getAllCombinations(num_iter_dom)
           num_select_dom= self.get_corresponding_domains(self.Domains,self.selectCond_combined,self.Selections)
           itertateOver = stateMachine.getAllCombinations(num_select_dom)
        else : # for denominator 
            num_iter_dom = self.get_corresponding_domains(self.Domains,self.selectCond_combined,self.Denominator_iterables)
            sumOver= stateMachine.getAllCombinations(num_iter_dom)#branch
            iter_over = self.remove_elements(self.Selections)
            num_select_dom= self.get_corresponding_domains(self.Domains,self.selectCond_combined,iter_over)
            itertateOver = stateMachine.getAllCombinations(num_select_dom)
 
        vals =[]
        condtionsLog =[]
        #[[0][1][2]]
        #itertateOver = [[0.0, 1.0]] remember #[C,E]  [[1],[0]]
        numIter = len(itertateOver)
        if numIter == 0:
            numIter = 1
        i=0
        while i <  numIter :
       # for i in range(0, len(itertateOver) ):
            for j in range(0, len(sumOver) ):
                
                condtions=""
                noms =[]
                doms =[]
                prod = 1
                try:
                    temp = itertateOver[i]
                except IndexError:
                    temp = []
               
                condtions = self.makeCondition(sumOver[j],location, temp) 
                nom= df.query(condtions)
                condtionsLog.append(condtions)
                noms.append(nom['P'].sum())
          
                condtions ,active = self.reduceQuery(condtions,location)
                while active :
                     dom= df.query(condtions)
                     doms.append(dom['P'].sum())
                     condtions ,active = self.reduceQuery(condtions,location)
                     nom= df.query(condtions)
                     noms.append(nom['P'].sum())
                     condtions ,active = self.reduceQuery(condtions,location) 
                doms.append(1)
                for k, w in zip(noms, doms): #P(B|A)P(A) ### P(B|A) = P(B,A)/P(A) 
                    w+=self.epsilon
                    prod *= (k / w)
                vals.append(prod)
            i+=1
        if location=='nom':
            return vals
        else:
            return sum(vals)     
        #pass
    def adjustNumIter(self, iterateOver, index, numIter):
        #[[0,1],[1],[1],[0,1]]
        #B was changing: [[0],[1]] because B can be 0, or 1
        #C and E are changing: [[0,0],[0,1],[1,0],[1,1]]
        #C and E are fixed at 1: [[1,1]]
        if not index >= len(iterateOver) and len(iterateOver[index]) <= 2:
            numIter = numIter - 1 - self.adjustNumIter(iterateOver, index + 1, numIter - 1)
            return numIter;
        else:
            return 0
            








