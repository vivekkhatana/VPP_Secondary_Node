# -*- coding: utf-8 -*-
"""
Created on Sun Oct 8 12:47:45 2023

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
    global Estar
    global droop_ratio
    global const
    global AvgConsFlag_ManChange
    global MaxConsFlag_ManChange
    global MaxConsInitFlag_ManChange
    global Y 
    global Z
    global Epsilon
    global Lambda
    global solDim
    global Max_myMax
    global Max_myMax_init
    global myRatio
    global Opt_complete

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
    
    AvgConsFlag = 1
    MaxConsFlag = 1
    MaxConsFlagInitial = 1
    AvgConsFlag_ManChange = 0
    MaxConsFlag_ManChange = 0
    MaxConsInitFlag_ManChange = 0
        
    Estar = 240*np.sqrt(2)
    droop_ratio = 2e-4
    const = 100
    
    solDim = 1
    
    Y = np.zeros(solDim)
    Z = np.zeros(solDim)
    Epsilon = np.ones(solDim)
    Lambda = np.zeros(solDim)
    
    Max_myMax = 0
    Max_myMax_init = 0
    myRatio = 0
    Opt_complete = 0


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
    global AvgConsFlag_ManChange 
    global MaxConsFlag_ManChange 
    global MaxConsInitFlag_ManChange
    global Y 
    global Z
    global Epsilon
    global Lambda
    global solDim
    global Opt_complete
    
    while threadGo:
        # logger.info(OPT_ON)
        if OPT_ON == 1:  

            # if AvgConsFlag == 1:
                
            # logger.info("inside optimization loop")     
                
            maxIter = 5; 
            
            Y = np.zeros(solDim)
            Z = np.zeros(solDim)
            Epsilon = np.ones(solDim)
            Lambda = np.zeros(solDim)
            
            # logger.info("latest alpha = " +str(alpha))
              
            rho = 1

            alpha = a1*(Estar - E_i) + a2*droop_ratio*Q_i;            
            
            opt_iter = 0
            myOpt_complete = 0
            
            logger.info("starting the optimization iterations with =" +str(alpha))   
            
            while (opt_iter < maxIter):  
                
                
                if ( (MaxConsFlag == 1) and (AvgConsFlag == 1) and (MaxConsFlagInitial == 1) and (myOpt_complete == 0) ): 
                    
                    logger.info("opt_iter = " +str(opt_iter))
                    
                    MaxConsFlag = 0
                    AvgConsFlag = 0
                    MaxConsFlagInitial = 0

                    
                    AvgConsFlag_ManChange = 1
                    MaxConsFlag_ManChange = 1  
                    MaxConsInitFlag_ManChange = 1

                    Y = (1/(1+rho))*( rho*Z + alpha - Lambda )
                    
                    # logger.info("Y =" +str(Y))
                    
                    dummy_var1 = (beta - 1)*droop_ratio*Q_i - gamma*(Estar - E_i) - (const*Epsilon)
                    
                    dummy_var = np.abs(Y)/np.abs(dummy_var1)

                    dummy_Proj = dummy_var/( np.maximum(1,dummy_var) )

                    Y = np.sign(Y)*dummy_Proj*np.abs(dummy_var1)

                    # logger.info("Y =" +str(Y))
                                        
                    Max_y = MaxConsInitialization(Y)
                    
                if (MaxConsFlagInitial == 1):   
                    
                    MaxConsFlagInitial = 0                    
                    MaxConsInitFlag_ManChange = 0
                    
                    # logger.info("Initialization done start Max consensus with =" +str(Max_y))
                    
                    Epsilon = MaxConsUpdate(Max_y)

                
                    if (MaxConsFlag == 1):
                        
                        # logger.info("Avg cons tolerance is =" +str(Epsilon))
                        
                        dummy_cons = Y + (1/rho)*Lambda
    
                        MaxConsFlag = 0
                        MaxConsFlag_ManChange = 0
                        
                        # logger.info("MaxConsFlag =" +str(MaxConsFlag))
                        
                        # logger.info("Max consensus is done. Starting avg consensus now with =" +str(dummy_cons))

                        Z = epsilonconsUpdate(dummy_cons, Epsilon)
                        
                        
                    if (AvgConsFlag == 1):

                                            
                        # logger.info("Avg consensus is done. Updating the dual variables")

                        logger.info("Z =" +str(Z))
                        
                        
                        MaxConsFlagInitial = 1                    
                        MaxConsInitFlag_ManChange = 0
                        MaxConsFlag = 1 
                        MaxConsFlag_ManChange = 0
                        
                        AvgConsFlag_ManChange = 0
                        
                        Lambda = Lambda + rho*(Y - Z) 
                        
                        # logger.info("Lambda =" +str(Lambda))
                
                        opt_iter += 1;
                    
            
            myOpt_complete = 1
            
            if (myOpt_complete == 1) and (Opt_complete == 0):
                basic_secondary_thread.opt_complete_flag_to_primary(myOpt_complete)
                basic_secondary_thread.opt_final_result_to_primary(float(Y))
                
            if (Opt_complete == 0):
                myOpt_complete = 0
                
                    
                     
# def startAvgConsensus():
    
#     global basic_secondary_thread
    
#     while threadGo:        
#         basic_secondary_thread.epsilonconsUpdate()
        
        
def MaxConsInitialization(newStartMaxConsVal_init):  
    
    global basic_secondary_thread
    global MaxConsFlagInitial
    global Max_myMax_init
    
    Max_init_iteration = 0
    myMaxConsflag_init = 0

    while True: 
        
            while ( Max_init_iteration < 2 ):

                if (Max_init_iteration == 0):
                    Max_myMax_init = newStartMaxConsVal_init
                    
                    # logger.info("newStartMaxConsVal = " +str(newStartMaxConsVal))
                    
                    myMax_init_Update = json.dumps({"myMaxConsUpdate": float(Max_myMax_init), "Max_iteration": Max_init_iteration, "NbrID": basic_secondary_thread.myID})   
                    # logger.info("self.Max_iteration = " +str(self.Max_iteration))
                    basic_secondary_thread.broadcastMaxto_OUT_Nbrs(myMax_init_Update) 
                    basic_secondary_thread.receiveMsgfrom_IN_Nbrs()
        
                elif (Max_init_iteration == 1):
        
                    dummy_initialize = []
                    initialization_vec = np.array(basic_secondary_thread.Max_IN_neighborMaxs)
                    
                    
                    for nbr_id in range(np.size(initialization_vec)):
                        dummy_val = np.abs( Max_myMax_init - initialization_vec[nbr_id] )
                        dummy_initialize = np.concatenate((np.array(dummy_initialize),np.array(dummy_val)),axis=None)

                    
                    if (len(dummy_initialize) != 0):                
                        Max_myMax_init = dummy_initialize.max()
                    else:
                        Max_myMax_init = Max_myMax_init 
                
                myMaxConsflag_init = 1  
                # logger.info("Sending max convergence flag to primary = " +str(self.myMaxConsflag))
                basic_secondary_thread.max_cons_init_flag_to_primary(myMaxConsflag_init)
                    
                Max_init_iteration += 1; 
                
            if (myMaxConsflag_init == 1):                
                if (MaxConsFlagInitial == 1):
                    # self.myMaxConsflag = 0
                    # logger.info("My new max consensus initial value is = " +str(Max_myMax_init))
                    return Max_myMax_init
                    exit()
                else:
                    myMax_init_Update = json.dumps({"myMaxConsUpdate": float(newStartMaxConsVal_init), "Max_iteration": Max_init_iteration, "NbrID": basic_secondary_thread.myID})   
                    # logger.info("self.Max_iteration = " +str(self.Max_iteration))
                    basic_secondary_thread.broadcastMaxto_OUT_Nbrs(myMax_init_Update) 
                    # logger.info(" My max consensus initialization is done waiting for others with value = " +str(Max_myMax_init))
                    basic_secondary_thread.max_cons_init_flag_to_primary(myMaxConsflag_init)

            

def MaxConsUpdate(newStartMaxConsVal):  
    
    global basic_secondary_thread
    global MaxConsFlag
    global Max_myMax
    
    Max_iteration = 0
    myMaxConsflag = 0
    Max_myMax = newStartMaxConsVal

    while True: 

        while (Max_iteration <= basic_secondary_thread.Diam + 2):
                
            myMax_Update = json.dumps({"myMaxConsUpdate": float(Max_myMax), "Max_iteration": Max_iteration, "NbrID": basic_secondary_thread.myID})  
            
            # logger.info("New max consensus update = " +str(myMax_Update))
             
            basic_secondary_thread.broadcastMaxto_OUT_Nbrs(myMax_Update)                    
            basic_secondary_thread.receiveMsgfrom_IN_Nbrs()
                
            dummy_myMax = np.array(basic_secondary_thread.Max_IN_neighborMaxs)  


            if (len(dummy_myMax) != 0):                
                 Max_myMax = np.maximum(Max_myMax, dummy_myMax.max())   
            else:
                 Max_myMax = Max_myMax 

            Max_iteration += 1; 
            time.sleep(0.01)
                        
        myMaxConsflag = 1    

        # logger.info("Sending max convergence flag to primary = " +str(self.myMaxConsflag))
        basic_secondary_thread.max_cons_flag_to_primary(myMaxConsflag) 
        
        if (myMaxConsflag == 1):
            if (MaxConsFlag == 1):
                # self.myMaxConsflag = 0
                # logger.info(" The max consensus code should exit now.")
                return Max_myMax
                exit()
            else:
                # logger.info("Sending max convergence flag to primary = " +str(self.myMaxConsflag))
                basic_secondary_thread.max_cons_flag_to_primary(myMaxConsflag)
                myMax_Update = json.dumps({"myMaxConsUpdate": float(Max_myMax), "Max_iteration": Max_iteration, "NbrID": basic_secondary_thread.myID})  
                basic_secondary_thread.broadcastMaxto_OUT_Nbrs(myMax_Update) 


def epsilonconsUpdate(newStartConsVal, consTol):
    
    global basic_secondary_thread
    global AvgConsFlag
    global myRatio
     
    outDeg = len(basic_secondary_thread.NbrIDs)
    weight = 1/(outDeg + 1)
    
    diff = 1000;
    
    
    # logger.info("asked to stop at = " + str(self.myRatio))
    myNum = newStartConsVal
    myDen = 1
    myRatio = myNum
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
    
    Avg_iteration = 0
    basic_secondary_thread.IN_neighborMaxs = []
    basic_secondary_thread.IN_neighborMins = [] 
    basic_secondary_thread.IN_neighborNumsSum = []
    basic_secondary_thread.IN_neighborDensSum = []
    
    basic_secondary_thread.OutBufNums = []
    basic_secondary_thread.OutBufDens = []
    basic_secondary_thread.OutBufMaxs = []
    basic_secondary_thread.OutBufMins = []
        
        
    
    logger.info("Starting new average consensus with the initial value = " +str(myNum))
    
    while True: 
                    
        
        if (AvgConsFlag == 1):
            # if (myAvgConsflag == 1):
                logger.info(" My converged avg cons value=" +str(myRatio))                
                return myRatio
                # if (outDeg == 2):
                #     time.sleep(0.001)
                exit()
                
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
                        
                        myMax = myRatio
                        myMin = myRatio
    
                    else:                
                        myMax = myRatio
                        myMin = myRatio



def recMeas_from_primary():  
    global basic_secondary_thread
    global beta
    global gamma
    global Q_i
    global E_i
    global a1
    global a2
    global OPT_ON
    global AvgConsFlag
    global MaxConsFlag
    global MaxConsFlagInitial
    global AvgConsFlag_ManChange 
    global MaxConsFlag_ManChange 
    global MaxConsInitFlag_ManChange
    global Opt_complete
    
    while threadGo:
        message = basic_secondary_thread.connection_handler.get_message() 
        
        if message['type'] == 'latest_parameters':
            # logger.info("msg = " +str(message['payload']))
            if abs(np.asarray(message['payload']).any()) > 0:
                vector_to_make_meas = message['payload']
                Q_i = vector_to_make_meas[0]
                E_i = vector_to_make_meas[1] 
                a1 = vector_to_make_meas[2] 
                a2 = vector_to_make_meas[3] 
                beta = vector_to_make_meas[4] 
                gamma = vector_to_make_meas[5] 
                OPT_ON = 1
                # logger.info("new measurement =" +str(vector_to_make_meas)) 
        elif message['type'] == 'avg_cons_stop_flag': 
            if (AvgConsFlag_ManChange == 1):
                if (AvgConsFlag != message['payload']):
                    AvgConsFlag = message['payload']
            # logger.info("AvgConsFlag =" +str(AvgConsFlag))
        elif message['type'] == 'max_cons_stop_flag':
            # logger.info("I am getting the max cons flag")
            if (MaxConsFlag_ManChange == 1):
                if (MaxConsFlag != message['payload']):
                    MaxConsFlag = message['payload']
            # logger.info("Received MaxConsFlag =" +str(MaxConsFlag))
        elif message['type'] == 'max_cons_init_stop_flag':
            # logger.info("I am getting the max cons flag")
            if (MaxConsInitFlag_ManChange == 1):
                if (MaxConsFlagInitial != message['payload']):
                    MaxConsFlagInitial = message['payload']
            # logger.info("Received MaxConsFlag =" +str(MaxConsFlag))
        elif message['type'] == 'opt_complete_flag':
            # logger.info("I am getting the max cons flag")
            Opt_complete = message['payload']
            # logger.info("Received MaxConsFlag =" +str(MaxConsFlag))
            

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
    
    # thread3 = Thread(target=startAvgConsensus)
    # thread3.daemon = True
    # thread3.start() # start running consensus
    
    # thread4 = Thread(target=startMaxConsensus)
    # thread4.daemon = True
    # thread4.start() # start running consensus
    
    
    ## do infinite while to keep main alive so that logging from threads shows in console
    try:
        while True:
            time.sleep(0.001)
    except KeyboardInterrupt:
        sys.exit()
        logger.info("cleaning up threads and exiting...")
        threadGo = False
        logger.info("done.")
