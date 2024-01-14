# -*- coding: utf-8 -*-
"""
Created on Fri Dec 29 17:44:20 2023

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
    global beta
    global gamma
    global Q_i
    global E_i
    global a1
    global a2
    global AvgConsFlag
    global MaxConsFlag
    global MaxConsFlagInitial
    global AvgConsFlagInitial
    global myMaxConsflag
    global myAvgConsflag
    global myMaxConsInitflag
    global Estar
    global droop_ratio
    global const
    
    global AvgConsFlag_ManChange
    global MaxConsFlag_ManChange
    global MaxConsInitFlag_ManChange
    global AvgConsInitFlag_ManChange
    global StartNewCons
    global StartNewMaxCons
    global StartNewMaxConsInit
    global Opt_complete
    global myOpt_complete
    
    global Y 
    global Z
    global Epsilon
    global Lambda
    global solDim
    global Max_myMax
    global Max_myMax_init
    global myRatio
    global converged_value

    global newStartConsVal 
    global newStartMaxConsVal
    global newStartMaxConsVal_init
    global MaxConsensusValue
    global AvgConsensusValue
    
    global Avg_iteration
    global Max_iteration
    global Max_init_iteration

    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'mastercluster_opal_SecControl.txt')) # this gives the address of the primary that we want to connect to 
    
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
    
    Q_i = 0  
    E_i = 0
    beta = 0
    gamma = 0
    a1 = 0
    a2 = 0    
    
    AvgConsFlag = 0
    MaxConsFlag = 0
    MaxConsFlagInitial = 0
    AvgConsFlagInitial = 0
    AvgConsFlag_ManChange = -1
    MaxConsFlag_ManChange = -1
    MaxConsInitFlag_ManChange = -1
    AvgConsInitFlag_ManChange = -1
    
    myMaxConsflag = 0
    myAvgConsflag = 0
    myMaxConsInitflag = 0
    
    Opt_complete = 0
    myOpt_complete = 0
    
        
    Estar = 480*np.sqrt(2/3);
    droop_ratio = 1;
    const = 100
    converged_value = 0
    
    solDim = 1
    
    Y = np.zeros(solDim)
    Z = np.zeros(solDim)
    Epsilon = np.ones(solDim)
    Lambda = np.zeros(solDim)
    
    Max_myMax = -999
    Max_myMax_init = -999
    myRatio = 0
   
    StartNewCons = 0
    StartNewMaxCons = 0
    StartNewMaxConsInit = 0
     
    newStartConsVal = 0
    newStartMaxConsVal = 0
    newStartMaxConsVal_init = 0
    MaxConsensusValue = 0
    AvgConsensusValue = 0

    Avg_iteration = 0
    Max_iteration = 0
    Max_init_iteration = 0
    
    


def runOPT():
    
    global OPT_ON
    global solVec
    global basic_secondary_thread
    global beta
    global gamma
    global Q_i
    global E_i
    global a1
    global a2
    global Estar
    global droop_ratio
    global const
    global MaxConsFlag
    global AvgConsFlag
    global MaxConsFlagInitial
    global AvgConsFlagInitial
    global AvgConsFlag_ManChange 
    global MaxConsFlag_ManChange 
    global MaxConsInitFlag_ManChange
    global AvgConsInitFlag_ManChange
    
    global Y 
    global Z
    global Epsilon
    global Lambda
    global solDim
    global Opt_complete
    global myOpt_complete
    
    global StartNewCons
    global StartNewMaxCons
    global StartNewMaxConsInit
    
    global newStartConsVal 
    global newStartMaxConsVal
    global newStartMaxConsVal_init
    global MaxConsensusValue
    global AvgConsensusValue
    
    global Max_myMax_init
    global myRatio
    global Max_myMax
    global converged_value
    
    while threadGo:
        
        if OPT_ON == 1:  

            maxIter = 9; 
            
            Y = np.zeros(solDim)
            Z = np.zeros(solDim)
            Epsilon = np.ones(solDim)
            Lambda = np.zeros(solDim)
            
            # logger.info("latest alpha = " +str(alpha))
              
            rho = 1

            alpha = a1*(Estar - E_i) + a2*droop_ratio*Q_i;            
            
            opt_iter_old = -1
            opt_iter_new = 0
            myOpt_complete = 0
            Opt_complete = 0
            
            Max_init = 0
            Z_update = 0
            
            logger.info("starting the optimization iterations with =" +str(alpha))   
            
            if ( myOpt_complete == 0 ):
                
                while (opt_iter_new <= maxIter):

                    if (opt_iter_new < maxIter):

                        while (opt_iter_new != opt_iter_old):
                            
                            logger.info("opt_iter_new = " +str(opt_iter_new))
                                
                            Y = (1/(1+rho))*( rho*Z + alpha - Lambda )
                            
                            dummy_var1 = (beta - 1)*droop_ratio*Q_i - gamma*(Estar - E_i) - (const*Epsilon)
                            
                            dummy_var = np.abs(Y)/np.abs(dummy_var1)
        
                            dummy_Proj = dummy_var/( np.maximum(1,dummy_var) )
        
                            Y = np.sign(Y)*dummy_Proj*np.abs(dummy_var1)
        
                            logger.info("Y =" +str(Y))

                            Max_init = 0
                            Z_update = 0
                             
                            opt_iter_old = opt_iter_new
                                              
                        while (opt_iter_new == opt_iter_old):
                            
                            if ( Max_init == 0 ):
                                
                                dummy_cons = Y + (1/rho)*Lambda 

                                dummy_cons_str = str(dummy_cons)
                                dummy_cons_str = dummy_cons_str.strip('.]').strip('.[')
                                dummy_cons_float = float(dummy_cons_str)
    
                                basic_secondary_thread.avg_cons_init_value_to_primary(dummy_cons_float)
                                basic_secondary_thread.avg_cons_init_flag_to_primary(1)

                                Max_init = 1
                                
                                AvgConsFlagInitial = 0
                                AvgConsInitFlag_ManChange = 0
    
                                time.sleep(5)
                                        
                            if ( (AvgConsFlagInitial == 1) and (Z_update == 0) ):
        
                                Z = AvgConsensusValue
                                
                                # logger.info("Z =" +str(Z))
                                
                                Lambda = Lambda + rho*(Y - Z)
                                
                                # logger.info("Lambda =" +str(Lambda))
        
                                Z_update = 1
                                opt_iter_new = opt_iter_new + 1;
                                AvgConsFlagInitial = 0
                                AvgConsInitFlag_ManChange = 1

                                time.sleep(1)
            
                    if (opt_iter_new == maxIter):
                        
                        while (opt_iter_new != opt_iter_old):
                            
                            logger.info("this is the last iter = " +str(opt_iter_new))
                                
                            Y = (1/(1+rho))*( rho*Z + alpha - Lambda )
                            
                            dummy_var1 = (beta - 1)*droop_ratio*Q_i - gamma*(Estar - E_i) - (const*Epsilon)
                            
                            dummy_var = np.abs(Y)/np.abs(dummy_var1)
        
                            dummy_Proj = dummy_var/( np.maximum(1,dummy_var) )
        
                            Y = np.sign(Y)*dummy_Proj*np.abs(dummy_var1)
        
                            logger.info("Y =" +str(Y))
                        
                            MaxConsFlagInitial = 0 
                            # Y_str = str(Y)
                            # Y_str = Y_str.strip('.]').strip('.[')
                            # Y_float = float(Y_str)
                            # basic_secondary_thread.max_cons_init_value_to_primary(Y_float)
                            # basic_secondary_thread.max_cons_init_flag_to_primary(1)
                            
                            Max_init = 0
                            Z_update = 0
                             
                            opt_iter_old = opt_iter_new
                                              
                        while (opt_iter_new == opt_iter_old):

                            if ( Max_init == 0 ):
                                
                                AvgConsFlag_ManChange = 1
                                dummy_cons = Y + (1/rho)*Lambda 
                                
                                dummy_cons_str = str(dummy_cons)
                                dummy_cons_str = dummy_cons_str.strip('.]').strip('.[')
                                dummy_cons_float = float(dummy_cons_str)

                                StartNewCons = 1
                                newStartConsVal = dummy_cons_float
                                # MaxConsFlagInitial = 0
                                Max_init = 1
                                AvgConsFlag = 0
                                AvgConsFlag_ManChange = 0
    
                                time.sleep(5)
                                        
                            if ( (AvgConsFlag == 1) and (Z_update == 0) ):
        
                                Z = AvgConsensusValue
                                
                                Lambda = Lambda + rho*(Y - Z) 
        
                                Z_update = 1
                                opt_iter_new = opt_iter_new + 1;
                                AvgConsFlag = 0
                                time.sleep(1)
                        

            myOpt_complete = 1
            converged_value = float(Y)
            time.sleep(5)
                

def epsilonconsUpdate():
    
    global basic_secondary_thread
    global myRatio
    global Avg_iteration
    global myAvgConsflag
    global myRatio
    global StartNewCons
    global newStartConsVal
    global Epsilon
    global Max_myMax
    global AvgConsFlag
    global AvgConsFlag_ManChange
     
    outDeg = len(basic_secondary_thread.NbrIDs)
    weight = 1/(outDeg + 1)
    
    diff = 1000;     
        
    myNum = 0
    myDen = 0
    myRatio = -999
    myMax = myNum
    myMin = myNum
    myMXP = 1000
    myMNP = 0
    myNumSumOld = 0
    myDenSumOld = 0  
    myNbrNumSumOld = 0
    myNbrDenSumOld = 0
    
    consTol = 1e-2
    
    Avg_iteration = 0
    myAvgConsflag = 0
    basic_secondary_thread.IN_neighborMaxs = []
    basic_secondary_thread.IN_neighborMins = [] 
    basic_secondary_thread.IN_neighborNumsSum = []
    basic_secondary_thread.IN_neighborDensSum = []
    
    basic_secondary_thread.OutBufNums = []
    basic_secondary_thread.OutBufDens = []
    basic_secondary_thread.OutBufMaxs = []
    basic_secondary_thread.OutBufMins = []

    # logger.info("Starting new average consensus with the initial value = " +str(myNum))
    
    while True: 
        
        if (StartNewCons == 1):
            diff = 1000;     
        
            myNum = newStartConsVal
            myDen = 1
            myRatio = myNum
            myMax = myNum
            myMin = myNum
            myMXP = 1000
            myMNP = 0
            myNumSumOld = 0
            myDenSumOld = 0  
            myNbrNumSumOld = 0
            myNbrDenSumOld = 0
            
            consTol = 1e-2
            
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
            
            myAvgConsflag = 0
            
            time.sleep(0.1)
            # AvgConsFlag_ManChange = 0

            logger.info("Starting new average consensus with the initial value = " +str(myRatio))
            
        else: 
            if (myRatio == -999):
                # logger.info("Waiting for new Avg cons")
                pass
            else:
                if (myAvgConsflag == 1):

                    basic_secondary_thread.avg_cons_flag_to_primary(myAvgConsflag)
    
                    n = float(myNum)
                    d = float(myDen)
                    
                    M = myMax
                    M = float(M)
                    
                    m = myMin                    
                    m = float(m)
    
                    myUpdate = json.dumps({"numerator": n, "denominator": d, "max": M, "min": m, "iteration": Avg_iteration, "NbrID": basic_secondary_thread.myID})
     
                    basic_secondary_thread.broadcastMsgto_OUT_Nbrs(myUpdate)
                    
                    # basic_secondary_thread.receiveMsgfrom_IN_Nbrs()
                    
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
        
                    myRatio = np.divide(myNum, myDen)
                    
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
                        # logger.info("my converged value = " + str(self.myRatio))
                        
                        # logger.info("diff = " +str(diff))
                        
                        if (diff < consTol):
                            # logger.info("seems like consensus is reached" + str(self.myRatio)) 
                            if (myMXP == 0 and myMNP == 0):
                                pass
                            else:
                                myAvgConsflag = 1
                                basic_secondary_thread.avg_cons_flag_to_primary(myAvgConsflag)
                                AvgConsFlag_ManChange = 0
                            
                            myMax = myRatio
                            myMin = myRatio
        
                        else:                
                            myMax = myRatio
                            myMin = myRatio


def startAvgConsensus():
    
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
        # logger.info("my converged value is =" +str(Y))
        basic_secondary_thread.opt_final_result_to_primary(converged_value)
        myOpt_complete = 0
        Opt_complete = 0
        time.sleep(1)     


def recMeas_from_primary():  
    global basic_secondary_thread
    global beta
    global gamma
    global Q_i
    global E_i
    global a1
    global a2
    global droop_ratio
    global OPT_ON
    global AvgConsFlag
    global MaxConsFlag
    global MaxConsFlagInitial
    global Opt_complete
    global AvgConsFlag_ManChange
    global MaxConsFlag_ManChange
    global MaxConsInitFlag_ManChange
    global MaxConsensusValue
    global AvgConsensusValue
    global AvgConsFlagInitial
    global AvgConsInitFlag_ManChange
    
    while threadGo:
        message = basic_secondary_thread.connection_handler.get_message() 
        
        if message['type'] == 'latest_parameters':
            # logger.info("msg = " +str(message['payload']))
            if abs(np.asarray(message['payload']).any()) > 0:
                vector_to_make_meas = message['payload']
                E_i = vector_to_make_meas[0]
                Q_i = vector_to_make_meas[1] 
                a1 = vector_to_make_meas[2] 
                a2 = vector_to_make_meas[3] 
                beta = vector_to_make_meas[4] 
                gamma = vector_to_make_meas[5] 
                droop_ratio = vector_to_make_meas[6]
                OPT_ON = 1
                # logger.info("new measurement =" +str(vector_to_make_meas)) 
        elif message['type'] == 'avg_cons_stop_flag':  
            AvgConsFlag = message['payload']
            if (AvgConsFlag_ManChange == 1):
                AvgConsFlag = 0
            # logger.info("AvgConsFlag =" +str(AvgConsFlag))
        elif message['type'] == 'max_cons_stop_flag':
            MaxConsFlag = message['payload']
            # if (MaxConsFlag_ManChange == 0):
            #     MaxConsFlag = 0
            # logger.info("Received MaxConsFlag =" +str(MaxConsFlag))
        elif message['type'] == 'max_cons_init_stop_flag':
            MaxConsFlagInitial = message['payload']
            # if (MaxConsInitFlag_ManChange == 0):
            #     MaxConsFlagInitial = 0            
            # logger.info("Received MaxConsFlagInitial =" +str(MaxConsFlagInitial))
        elif message['type'] == 'avg_cons_init_stop_flag':
            AvgConsFlagInitial = message['payload']
            if (AvgConsInitFlag_ManChange == 1):
                AvgConsFlagInitial = 0            
            # logger.info("Received MaxConsFlagInitial =" +str(MaxConsFlagInitial))
        elif message['type'] == 'max_consensus_value':
            MaxConsensusValue = message['payload']    
            # logger.info("Received MaxConsensusValue =" +str(MaxConsensusValue))            
        elif  message['type'] == 'avg_consensus_value':
            AvgConsensusValue = message['payload']    
            # logger.info("Received MaxConsensusValue =" +str(MaxConsensusValue))
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
    
    thread3 = Thread(target=startAvgConsensus)
    thread3.daemon = True
    thread3.start() # start running consensus
    
    thread4 = Thread(target=sendUpdates)
    thread4.daemon = True
    thread4.start() # start running consensus
    
    # thread5 = Thread(target=startMaxConsensus)
    # thread5.daemon = True
    # thread5.start() # start running consensus
    
    
    ## do infinite while to keep main alive so that logging from threads shows in console
    try:
        while True:
            time.sleep(0.001)
    except KeyboardInterrupt:
        sys.exit()
        logger.info("cleaning up threads and exiting...")
        threadGo = False
        logger.info("done.")
