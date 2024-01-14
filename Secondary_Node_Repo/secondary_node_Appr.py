# -*- coding: utf-8 -*-
"""
Created on Sat Dec 16 17:18:45 2023

@author: khata010
"""
#!/usr/bin/env python3

import os
import json
from threading import Thread
import time
import sys
import configparser
import numpy as np
from RpiCluster.MainLogger import logger
from RpiCluster.SecondaryNodes.RpiBasicSecondaryThread import RpiBasicSecondaryThread
from RpiCluster.NodeConfig import NodeConfig


def start_secondary():    
    
    global basic_secondary_thread
    global solVec
    global OPT_ON 
    global AvgConsFlag
    global Avg_iteration
    global myAvgConsflag
    global Z
    global Lambda
    global rhoD
    global PI_max
    global PI_min
    global solDim
    global myRatio
    global Opt_complete 
    global StartNewCons
    global newStartConsVal

    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'mastercluster_opal_Appr.txt')) # this gives the address of the primary that we want to connect to 
    
    NodeConfig.load(config)
    
    primary_port = config.getint("secondary", "primary_port")
    primary_ip = config.get("secondary", "primary_ip")
    
    # This class creates and runs a basic secondary thread
    basic_secondary_thread = RpiBasicSecondaryThread(primary_ip, primary_port)
    basic_secondary_thread.start()
    basic_secondary_thread.getNbrInfo() 
    basic_secondary_thread.bindSecondary()             
    solVec = 0

    OPT_ON = 0 
    rhoD = 0
    PI_max = 0
    PI_min = 0
    solDim = 1
    
    AvgConsFlag = 0
    myAvgConsflag = 0
    
    Z = np.zeros(solDim)
    
    myRatio = 0
    Opt_complete = 0
    StartNewCons = 0
    newStartConsVal = 0
    Avg_iteration = 0



def runOPT():
    global OPT_ON
    global solVec
    global basic_secondary_thread
    global AvgConsFlag
    global Z
    global solDim
    global Opt_complete
    global rhoD
    global StartNewCons
    global newStartConsVal
    global myRatio
    
    
    
    while threadGo:
        # logger.info(OPT_ON)
        if OPT_ON == 1: 
                                   
            power_demand = rhoD
                         
            if (Opt_complete == 0): 
                
                logger.info("I started the apportioning iterations with power_demand = " +str(power_demand))
                
                Opt_complete = 1
                
                dummy_cons = power_demand
                 
                StartNewCons = 1    
                newStartConsVal = dummy_cons
                

                     


def epsilonconsUpdate():
    
    global basic_secondary_thread
    global Avg_iteration
    global myAvgConsflag
    global myRatio
    global PI_max
    global PI_min
    global StartNewCons
    global newStartConsVal
    
    
    outDeg = len(basic_secondary_thread.NbrIDs)
    weight = 1/(outDeg + 1)
    
    diff = 1000;     
        
    myNum = 0
    myDen = 0
    myRatio = myNum
    myMax = myNum
    myMin = myNum
    myMXP = 1000
    myMNP = 0
    myNumSumOld = 0
    myDenSumOld = 0  
    myNbrNumSumOld = 0
    myNbrDenSumOld = 0
    
    consTol = 1e-3
    
    Avg_iteration = 0
    basic_secondary_thread.IN_neighborMaxs = []
    basic_secondary_thread.IN_neighborMins = [] 
    basic_secondary_thread.IN_neighborNumsSum = []
    basic_secondary_thread.IN_neighborDensSum = []
    
    basic_secondary_thread.OutBufNums = []
    basic_secondary_thread.OutBufDens = []
    basic_secondary_thread.OutBufMaxs = []
    basic_secondary_thread.OutBufMins = []

    
    while True: 
        
        if (StartNewCons == 1):
            # logger.info("asked to stop at = " + str(self.myRatio))
            myNum = newStartConsVal - PI_min
            myDen = PI_max - PI_min
            myRatio = myNum/myDen
            myMax = myNum
            myMin = myNum
            myAvgConsflag = 0
            myMXP = 1000
            myMNP = 0
            myNumSumOld = 0
            myDenSumOld = 0  
            myNbrNumSumOld = 0
            myNbrDenSumOld = 0
            diff = 1000
                       
            consTol = 1e-3
            
            Avg_iteration = 0
            basic_secondary_thread.IN_neighborMaxs = []
            basic_secondary_thread.IN_neighborMins = [] 
            basic_secondary_thread.IN_neighborNumsSum = []
            basic_secondary_thread.IN_neighborDensSum = []
            
            basic_secondary_thread.OutBufNums = []
            basic_secondary_thread.OutBufDens = []
            basic_secondary_thread.OutBufMaxs = []
            basic_secondary_thread.OutBufMins = []
            
            StartNewCons = 0

            # logger.info("Starting new average consensus with the initial value = " +str(myRatio))
            time.sleep(0.1)
        
        else: 
            if (myDen == 0):
                pass
            else:            
            
                if (myAvgConsflag == 1):            
                    # logger.info("Sending max convergence flag to primary = " +str(myAvgConsflag))
                    basic_secondary_thread.avg_cons_flag_to_primary(myAvgConsflag)
    
                    n = float(myNum)
                    d = float(myDen)
                    
                    M = myMax
                    M = float(M)
                    
                    m = myMin                    
                    m = float(m)
    
                    myUpdate = json.dumps({"numerator": n, "denominator": d, "max": M, "min": m, "iteration": Avg_iteration, "NbrID": basic_secondary_thread.myID})
     
                    basic_secondary_thread.broadcastMsgto_OUT_Nbrs(myUpdate)
                    
                    basic_secondary_thread.receiveMsgfrom_IN_Nbrs()
                    
                    
                    
                    # logger.info(" My Avg consensus converged waiting for others with value = " +str(myRatio))
    
                else: 
        
                    Avg_iteration += 1;  
                                    
                    basic_secondary_thread.OutBufNums = np.concatenate((np.array(basic_secondary_thread.OutBufNums),np.multiply(myNum,weight)),axis=None)
                    basic_secondary_thread.OutBufDens = np.concatenate((np.array(basic_secondary_thread.OutBufDens),np.multiply(myDen,weight)),axis=None)
                    basic_secondary_thread.OutBufMaxs = np.concatenate((np.array(basic_secondary_thread.OutBufMaxs),myMax),axis=None)
                    basic_secondary_thread.OutBufMins = np.concatenate((np.array(basic_secondary_thread.OutBufMins),myMin),axis=None) 
                    
                    n = np.sum(basic_secondary_thread.OutBufNums)                    
                    d = np.sum(basic_secondary_thread.OutBufDens) 
        
                    n = float(n)
                    d = float(d)
                    
                    M = myMax
                    M = float(M)
                    
                    m = myMin                    
                    m = float(m)
                    
            
                    myUpdate = json.dumps({"numerator": n, "denominator": d, "max": M, "min": m, "iteration": Avg_iteration, "NbrID": basic_secondary_thread.myID})
         
                    # logger.info("myUpdate" +str(myUpdate))        
         
                    basic_secondary_thread.broadcastMsgto_OUT_Nbrs(myUpdate)
                    
                    basic_secondary_thread.receiveMsgfrom_IN_Nbrs()
        
                    
                    tempNumSum = np.sum(basic_secondary_thread.OutBufNums)
                    tempDenSum = np.sum(basic_secondary_thread.OutBufDens)
                
                    myNum = (tempNumSum - myNumSumOld) + (np.sum(basic_secondary_thread.IN_neighborNumsSum) - myNbrNumSumOld) 
                    myDen = (tempDenSum - myDenSumOld) + (np.sum(basic_secondary_thread.IN_neighborDensSum) - myNbrDenSumOld)
                    
                    myNumSumOld = tempNumSum
                    myDenSumOld = tempDenSum
        
                    myNbrNumSumOld = np.sum(basic_secondary_thread.IN_neighborNumsSum)
                    myNbrDenSumOld = np.sum(basic_secondary_thread.IN_neighborDensSum)
                    
                    # np.seterr(invalid='ignore')
                    
                    if (myDen != 0):
                        myRatio = np.divide(myNum, myDen)
                        # logger.info("myRatio =" +str(myRatio))
                    
                    dummyMax = np.array(basic_secondary_thread.IN_neighborMaxs)
                    dummyMin = np.array(basic_secondary_thread.IN_neighborMins)
                    
                    
                    if (len(dummyMax) != 0):                
                        myMax = np.maximum(myMax, dummyMax.max())
                        myMin = np.minimum(myMin, dummyMin.min())
                    else:
                        myMax = myMax
                        myMin = myMin
        
                    if (np.mod(Avg_iteration, basic_secondary_thread.Diam) == 0):
                        
                        myMXP = myMax
                        myMNP = myMin
        
                        diff = np.abs(myMXP - myMNP)
                                           
                        if (diff < consTol):
                            # logger.info("seems like consensus is reached" + str(self.myRatio)) 
                            if (myMXP == 0 and myMNP == 0):
                                pass
                            else:
                                myAvgConsflag = 1
                                logger.info("myID =" +str(basic_secondary_thread.myID)) 
                                logger.info("myAvgConsflag =" +str(myAvgConsflag)) 
                                basic_secondary_thread.avg_cons_flag_to_primary(myAvgConsflag)
                            
                            myMax = myRatio
                            myMin = myRatio
        
                        else:                
                            myMax = myRatio
                            myMin = myRatio
                    
                    
def startConsensus():
    
    while threadGo:        
        epsilonconsUpdate()


def recMeas_from_primary():  
    global basic_secondary_thread
    global PI_max
    global PI_min
    global OPT_ON
    global AvgConsFlag
    global Opt_complete
    global rhoD
    
    while threadGo:
        message = basic_secondary_thread.connection_handler.get_message() 
        
        if message['type'] == 'latest_parameters':
            # logger.info("msg = " +str(message['payload']))
            if abs(np.asarray(message['payload']).any()) > 0:
                vector_to_make_meas = message['payload']
                rhoD = vector_to_make_meas[0]
                PI_max = vector_to_make_meas[1]
                PI_min = vector_to_make_meas[2] 
                OPT_ON = 1          
        elif message['type'] == 'avg_cons_stop_flag': 
            # if (AvgConsFlag_ManChange == 1):
                # if (AvgConsFlag != message['payload']):
            AvgConsFlag = message['payload']  
            # logger.info("Received AvgConsFlag =" +str(AvgConsFlag))  
            if (AvgConsFlag == 1):
                    
                # logger.info("AvgConsFlag =" +str(AvgConsFlag))
                                    
                Opt_complete = 1
                
                basic_secondary_thread.opt_complete_flag_to_primary(1)
                Z = myRatio
                basic_secondary_thread.opt_final_result_to_primary(float(Z))
                    
        elif message['type'] == 'opt_complete_flag':
            # logger.info("I am getting the max cons flag")
            Opt_complete = message['payload']
            
                

if __name__ == "__main__": 
    
    initSuccess = start_secondary()
    
    threadGo = True #set to false to quit threads
    # start a new thread to start the optimization algorithm    
    thread1 = Thread(target=runOPT)
    thread1.daemon = True
    thread1.start()
    
    thread2 = Thread(target=recMeas_from_primary)
    thread2.daemon = True
    thread2.start()   # Receive latest measurement from primary node
    
    thread3 = Thread(target=startConsensus)
    thread3.daemon = True
    thread3.start() # start running consensus
    
    
    ## do infinite while to keep main alive so that logging from threads shows in console
    try:
        while True:
            time.sleep(0.001)
    except KeyboardInterrupt:
        sys.exit()
        logger.info("cleaning up threads and exiting...")
        threadGo = False
        logger.info("done.")
