#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 17 08:51:51 2024
a helper file for the Selection_MAR.py file for summing over the active domains 
"""

class NodeList():
    def __init__(self, val_list):
        #domains will be passed in here
        self.nodeValues = val_list
        self.index = 0
    
    def getVal(self):
        return self.nodeValues[self.index]
    
    def incrementIndex(self):
        if self.index == len(self.nodeValues) - 1:
            #index is already at the end
            return False
        else:
            self.index += 1
            return True
        
    def resetIndex(self):
        self.index = 0
        
    def isAtEnd(self):
        if self.index == len(self.nodeValues) - 1:
            #index is already at the end
            return True
        else:
            return False
        

class StateMachine():
    def __init__(self, debug_values = []):
        self.debug_values = debug_values
        self.currentCombinationValues = []
        pass
    
    def addCombination(self, val_list, result):
        result.append(val_list)
        
    def isAtEndAllNodes(self, node_list):
        flag = 0
        for node in node_list:
            if not node.isAtEnd():
                return False
                flag = 1
                break
        if flag == 0:
            return True
        
    def iterateNode(self, node_list):
        for i in range(0, len(node_list)):
            if not node_list[i].isAtEnd():
                node_list[i].incrementIndex()
                #this goes to the next node in the list if this one is at the end
                break
            else:
                node_list[i].resetIndex()
                
        
    def addCombination(self, node_list, debug_mode = False):
        to_add = []
        for node in node_list:
            to_add.append(node.getVal())
# =============================================================================
#         if debug_mode:
#             print(to_add)
# =============================================================================
        self.currentCombinationValues.append(to_add)
    def getAllCombinations(self, domain_list):
        self.currentCombinationValues = []
        if self.debug_values != []:
            domain_list = self.debug_values
        if len(domain_list) == 0:
            return []
        nodes = []
        for arr in domain_list:
            nodes.append(NodeList(arr))
        while not self.isAtEndAllNodes(nodes):
            self.addCombination(nodes, 1)
            self.iterateNode(nodes[::-1])  # Reverse the order of iteration
        self.addCombination(nodes, 1)
        return self.currentCombinationValues
            
