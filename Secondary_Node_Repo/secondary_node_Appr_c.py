# -*- coding: utf-8 -*-
"""
Created on Sun Jan  7 15:38:28 2024

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
    global Lambda
    global rhoD
    global PI_max
    global PI_min
    global solDim
    global myRatio
    global Opt_complete
    global myOpt_complete
    global StartNewCons
    global newStartConsVal
    global AvgConsNumFlagInitial
    global AvgConsNumInitFlag_ManChange
    global AvgConsDenFlagInitial
    global AvgConsDenInitFlag_ManChange
    global AvgConsFlag_ManChange
    global Num
    global Den
    global converged_value
    global NumAvgConsensusValue
    global DenAvgConsensusValue

    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'mastercluster_opal_Appr_home.txt')) # this gives the address of the primary that we want to connect to 
    
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
    Num = 0
    Den = 1
    
    AvgConsFlag = 0
    myAvgConsflag = 0
        
    myRatio = 0
    Opt_complete = 0
    myOpt_complete = 0
    AvgConsNumFlagInitial = 0
    AvgConsNumInitFlag_ManChange = -1
    AvgConsDenFlagInitial = 0
    AvgConsDenInitFlag_ManChange = -1
    AvgConsFlag_ManChange = -1
    StartNewCons = 0
    newStartConsVal = 0
    Avg_iteration = 0
    converged_value = 0
    NumAvgConsensusValue = 0
    DenAvgConsensusValue = 1


def runOPT():
    global OPT_ON
    global solVec
    global basic_secondary_thread
    global AvgConsFlag
    global solDim
    global Opt_complete
    global myOpt_complete
    global rhoD
    global StartNewCons
    global newStartConsVal
    global AvgConsNumFlagInitial
    global AvgConsNumInitFlag_ManChange
    global AvgConsDenFlagInitial
    global AvgConsDenInitFlag_ManChange
    global AvgConsFlag_ManChange 
    global myRatio
    global PI_max
    global PI_min
    global Num
    global Den
    global converged_value
    global NumAvgConsensusValue
    global DenAvgConsensusValue

    while threadGo:
        
        if OPT_ON == 1: 
            
            Num = 0
            Den = 1
            Num_update = 0
            Intermediate_update_Num = 0
            Intermediate_update_Den = 0
            Den_update = 0
                                   
            power_demand = rhoD
            
             
            while ( myOpt_complete == 0 ):                 
                
                if (Intermediate_update_Num == 0):
                    
                    logger.info("I started the apportioning iterations with power_demand = " +str(power_demand))

                    dummy_cons_num = power_demand - PI_min
                    
                    # logger.info("dummy_cons_num = " +str(dummy_cons_num))

                    basic_secondary_thread.avg_cons_num_init_value_to_primary(dummy_cons_num)
                    basic_secondary_thread.avg_cons_num_init_flag_to_primary(1)
                    
                    Num_update = 0
                    AvgConsNumFlagInitial = 0
                    AvgConsNumInitFlag_ManChange = 0
                    Intermediate_update_Num = 1
                
                if ( (AvgConsNumFlagInitial == 1) and (Num_update == 0) ):
                            
                    Num = NumAvgConsensusValue
                    # logger.info("Num = " +str(Num))
                    AvgConsNumFlagInitial = 0
                    Num_update = 1
                    AvgConsNumInitFlag_ManChange = 1
                    
                                      
                if ( (Num_update == 1) and (Intermediate_update_Den == 0) ):
                                        
                    dummy_cons_den = PI_max - PI_min
                    # logger.info("dummy_cons_den = " +str(dummy_cons_den))
                    
                    basic_secondary_thread.avg_cons_den_init_value_to_primary(dummy_cons_den)
                    basic_secondary_thread.avg_cons_den_init_flag_to_primary(1)
                    Intermediate_update_Den = 1

                    AvgConsDenFlagInitial = 0
                    AvgConsDenInitFlag_ManChange = 0
                
                if ( (AvgConsDenFlagInitial == 1) and (Den_update == 0) ):
                            
                    Den = DenAvgConsensusValue
                    AvgConsDenFlagInitial = 0
                    Den_update = 1
                    AvgConsDenInitFlag_ManChange = 1
                    # Num_update = 0
                    # Intermediate_update = 0
                                        
                    # logger.info("Den = " +str(Den))
                    
                    myOpt_complete = 1
                    converged_value = Num/Den
                                      
                    time.sleep(1)

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


def sendUpdates():
    
    global converged_value
    
    while threadGo:        
        UpdatesToPrimary(converged_value) 
        
def UpdatesToPrimary(converged_value):
    global basic_secondary_thread
    global myOpt_complete
    global Opt_complete
    
    if (myOpt_complete == 1) and (Opt_complete == 0):
        basic_secondary_thread.opt_complete_flag_to_primary(myOpt_complete)
        time.sleep(1)

    if (Opt_complete == 1):
        # logger.info("my converged value is =" +str(converged_value))
        basic_secondary_thread.opt_final_result_to_primary(converged_value)
        myOpt_complete = 0
        Opt_complete = 0
        time.sleep(1)    



def recMeas_from_primary():  
    global basic_secondary_thread
    global PI_max
    global PI_min
    global OPT_ON
    global AvgConsFlag
    global AvgConsNumFlagInitial
    global AvgConsNumInitFlag_ManChange
    global AvgConsDenFlagInitial
    global AvgConsDenInitFlag_ManChange
    global AvgConsFlag_ManChange
    global AvgConsensusValue
    global NumAvgConsensusValue
    global DenAvgConsensusValue
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
            AvgConsFlag = message['payload']
            if (AvgConsFlag_ManChange == 1):
                AvgConsFlag = 0  
        elif message['type'] == 'avg_num_cons_init_stop_flag':
            AvgConsNumFlagInitial = message['payload']
            if (AvgConsNumInitFlag_ManChange == 1):
                AvgConsNumFlagInitial = 0            
            # logger.info("Received AvgConsNumFlagInitial =" +str(AvgConsNumFlagInitial))
        elif message['type'] == 'avg_den_cons_init_stop_flag':
            AvgConsDenFlagInitial = message['payload']
            if (AvgConsDenInitFlag_ManChange == 1):
                AvgConsDenFlagInitial = 0            
            # logger.info("Received AvgConsNumFlagInitial =" +str(AvgConsNumFlagInitial)) 
        elif  message['type'] == 'avg_consensus_value':
            AvgConsensusValue = message['payload']    
            # logger.info("Received AvgConsensusValue =" +str(AvgConsensusValue))
        elif  message['type'] == 'num_avg_consensus_value':
            NumAvgConsensusValue = message['payload']    
            # logger.info("Received AvgConsensusValue =" +str(AvgConsensusValue))
        elif  message['type'] == 'den_avg_consensus_value':
            DenAvgConsensusValue = message['payload']    
            # logger.info("Received DenAvgConsensusValue =" +str(DenAvgConsensusValue))
        elif message['type'] == 'opt_complete_flag':
            # logger.info("I am getting the max cons flag")
            Opt_complete = message['payload']
            # logger.info("Received Opt_complete =" +str(Opt_complete))
            
                

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
    
    # thread3 = Thread(target=startConsensus)
    # thread3.daemon = True
    # thread3.start() # start running consensus
    
    thread3 = Thread(target=sendUpdates)
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
