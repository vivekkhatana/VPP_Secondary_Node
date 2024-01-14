# -*- coding: utf-8 -*-
"""
Created on Wed Mar 22 20:22:11 2023

@author: khata010
"""

import threading
import time
import socket
import json
import numpy as np
from RpiCluster.MainLogger import logger
from RpiCluster.ConnectionHandler import ConnectionHandler
from RpiCluster.RpiClusterExceptions import DisconnectionException


class RpiBasicSecondaryThread(threading.Thread):
    """
        Creating children from this base will allow more complex systems to be created. This inherits from thread so
        you are able to start this running and perform other processing at the same time.

        Attributes:
            uuid and myID: A UUID and myID to represent the secondary thread. This is assigned by the primary
            primary_address: A tuple representing the IP address and port to use for the primary
            connection_handler: A connection handler object which will be used for sending/receiving messages.
    """
    def __init__(self, primary_ip, primary_port):
        threading.Thread.__init__(self)
        self.uuid = None
        self.myID = None
        self.NbrAdds = {} # list of out-neighbors' (including myself) addresses 
        self.NbrIDs = {}
                
        self.myNbrSockets = {} 
        self.myNbrAddresses = {}
        
        
        self.primary_address = (primary_ip, primary_port)
        self.connection_handler = None

        # self.my_address = {}
        self.my_listening_ip = None
        self.my_listening_port = None
        
        # self.functionInfo = {} # function parameters of the secondary
        
        self.my_listening_socket = None

        self.IN_neighborNumsSum = []
        self.IN_neighborDensSum = []
        self.IN_neighborMaxs = []
        self.IN_neighborMins = []
        self.OutBufNums = []
        self.OutBufDens = []
        self.OutBufMaxs = []
        self.OutBufMins = []
        
        self.myNum = 0
        self.myDen = 0
        self.myNumSumOld = 0
        self.myDenSumOld = 0  
        self.myNbrNumSumOld = 0
        self.myNbrDenSumOld = 0  
        self.myRatio = 0
        self.myMax = 0
        self.myMin = 0
        self.myMXP = 0
        self.myMNP = 0
        self.myAvgConsflag = 0
        self.myMaxConsflag = 0
        self.StartNewCons = 0     
        self.newStartConsVal = 0
        self.iteration = 0
        
        self.StartNewMaxCons = 0     
        self.newStartMaxConsVal = 0
        self.Max_IN_neighborMaxs = []
        self.Max_OutBufMaxs = []
         
        self.Max_myMax = 0
        self.Max_iteration = 0
        self.Max_counter = 0
        self.Avg_counter = 0
        
        self.consTol = 0.01

        self.Diam = {}

    def start(self):
        """Base method to begin running the thread, this will connect to the primary and then repeatedly call self.result_to_primary"""
        logger.info("Starting script...")

        while True:
            logger.info("Connecting to the primary...")
            connected = False
            while connected is False:
                try:
                    connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
                    connection.connect(self.primary_address)
                    self.my_listening_ip, self.my_listening_port = connection.getsockname()
                    # logger.info(self.my_listening_ip)
                    # logger.info(self.my_listening_port)
                    self.connection_handler = ConnectionHandler(connection)
                    connected = True
                except socket.error as e:
                    logger.info("Failed to connect to primary, waiting 20 seconds and trying again")
                    time.sleep(30)

            logger.info("Successfully connected to the primary")
            

            try:
                logger.info("Sending an initial hello to primary")
                # Getting initial settings
                self.connection_handler.send_message("nodeID", "info")
                message = self.connection_handler.get_message()
                self.myID = message['payload']
                logger.info("My assigned ID is " + str(self.myID))
                
                self.connection_handler.send_message("Diam", "info")
                message = self.connection_handler.get_message()
                self.Diam = message['payload']
                # logger.info("The network diameter is =" +str(self.Diam))                
                break;
                
            except DisconnectionException as e:
                logger.info("Got disconnection exception with message: " + e.message)
                logger.info("Secondary will try and reconnect once primary is back online")



    def getNbrInfo(self):
        
        while True:         
            message = self.connection_handler.get_message()         
            if message['type'] == 'NbrInfo':
                dummy = message['payload'] # We consider out-neighbors here
                
                self.NbrIDs = dummy['NbrIDs']
                self.NbrAdds = dummy['NbrAdds']
                # logger.info("my Nbr addresses")              
                # logger.info(self.NbrAdds)
                logger.info("my Nbr IDs")              
                logger.info(self.NbrIDs)
                break
                
                
    def bindSecondary(self):
        self.my_listening_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.my_listening_socket.bind((self.my_listening_ip, self.my_listening_port)) 
        self.my_listening_socket.listen(len(self.NbrIDs))
        myaddress = (self.my_listening_ip, self.my_listening_port)
        logger.info("I am bound to {address}".format(address=myaddress))                    

      
    def broadcastMsgto_OUT_Nbrs(self,Update):
                                  
        self.my_listening_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.my_listening_socket.bind((self.my_listening_ip, self.my_listening_port))  
        # logger.info("ready to connect to nbrs")
        self.my_listening_socket.listen(len(self.NbrIDs))  # listen to NbrInfo number of neighbor node-connects
        
        # Sockets to send messages to 
        for Nbr in range(len(self.NbrIDs)):
            with socket.socket() as Nbr_socket:
                try:                    
                    Nbr_address = tuple(self.NbrAdds[Nbr])
                    # logger.info("sent to nbr=" + str(Nbr_address))
                    Nbr_socket.connect(Nbr_address)
                    connection_handler_Nbr = ConnectionHandler(Nbr_socket)  
                    # logger.info("myUpdate =" +str(myUpdate))
                    connection_handler_Nbr.send_message(Update, "myUpdate")                                  
                except socket.error as e:
                    logger.info("Failed to connect to my out Nbr, waiting 1 seconds and trying again")
                    time.sleep(1)       
    
    def broadcastMaxto_OUT_Nbrs(self,Update):
                                  
        self.my_listening_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.my_listening_socket.bind((self.my_listening_ip, self.my_listening_port))  
        # logger.info("ready to connect to nbrs")
        self.my_listening_socket.listen(len(self.NbrIDs))  # listen to NbrInfo number of neighbor node-connects
        
        # Sockets to send messages to 
        for Nbr in range(len(self.NbrIDs)):
            with socket.socket() as Nbr_socket:
                try:                    
                    Nbr_address = tuple(self.NbrAdds[Nbr])
                    # logger.info("sent to nbr=" + str(Nbr_address))
                    Nbr_socket.connect(Nbr_address)
                    connection_handler_Nbr = ConnectionHandler(Nbr_socket)  
                    # logger.info("myUpdate =" +str(myUpdate))
                    connection_handler_Nbr.send_message(Update, "myMaxConsUpdate")                                   
                except socket.error as e:
                    logger.info("Failed to connect to my out Nbr, waiting 1 seconds and trying again")
                    time.sleep(1)
        # logger.info("Max cons outnbr sent")
    
    
    def receiveMsgfrom_IN_Nbrs(self):        
           
        self.my_listening_socket.listen(len(self.NbrIDs))  # listen to NbrInfo number of neighbor node-connects
        
        self.IN_neighborMaxs = []
        self.IN_neighborMins = []
        self.IN_neighborNumsSum = []
        self.IN_neighborDensSum = []
        
        usedNbrs = []
        
        self.Max_IN_neighborMaxs = []
        self.Max_OutBufMaxs = []
        
        ii = 0
        while True:
            
            connection, address = self.my_listening_socket.accept()
            connection_handler_Nbr = ConnectionHandler(connection) 
            data = connection_handler_Nbr.get_message()      
            if data['type'] == 'myUpdate':   
                dummy = json.loads(data['payload'])  
                # logger.info("self.iteration = " +str(self.iteration))
                # logger.info(dummy)
                if ( np.any(usedNbrs == dummy['NbrID']) ):
                    connection.close()
                else:
                    usedNbrs = np.concatenate((np.array(usedNbrs),np.array(dummy['NbrID'])),axis=None)
                    self.IN_neighborNumsSum = np.concatenate((np.array(self.IN_neighborNumsSum),np.array(dummy['numerator'])),axis=None)
                    self.IN_neighborDensSum = np.concatenate((np.array(self.IN_neighborDensSum),np.array(dummy['denominator'])),axis=None)
                    self.IN_neighborMaxs = np.concatenate((np.array(self.IN_neighborMaxs),np.array(dummy['max'])),axis=None)
                    self.IN_neighborMins = np.concatenate((np.array(self.IN_neighborMins),np.array(dummy['min'])),axis=None)
                    ii += 1;
                    connection.close()
                    
            if data['type'] == 'myMaxConsUpdate':
                dummy = json.loads(data['payload'])  
                # logger.info("self.iteration = " +str(self.iteration))
                # logger.info(dummy)
                if ( np.any(usedNbrs == dummy['NbrID']) ):
                    connection.close() 
                else:
                    usedNbrs = np.concatenate((np.array(usedNbrs),np.array(dummy['NbrID'])),axis=None)
                    self.Max_IN_neighborMaxs = np.concatenate((np.array(self.Max_IN_neighborMaxs),np.array(dummy['myMaxConsUpdate'])),axis=None)
                    ii += 1;
                    connection.close()
                
           
            if (ii == len(self.NbrIDs)):
                break
        
    
    def epsilonconsUpdate(self):  
        
        
        outDeg = len(self.NbrIDs)
        weight = 1/(outDeg + 1)

        diff = 1000;

        while True: 
            
            if (self.StartNewCons == 1):
                # logger.info("asked to stop at = " + str(self.myRatio))
                self.myNum = self.newStartConsVal
                self.myDen = 1
                self.myRatio = self.myNum
                self.myMax = self.myNum
                self.myMin = self.myNum
                self.myAvgConsflag = 0
                self.myMXP = 1000
                self.myMNP = 0
                self.myNumSumOld = 0
                self.myDenSumOld = 0  
                self.myNbrNumSumOld = 0
                self.myNbrDenSumOld = 0
                diff = 1000
                
                self.iteration = 0
                self.IN_neighborMaxs = []
                self.IN_neighborMins = [] 
                self.IN_neighborNumsSum = []
                self.IN_neighborDensSum = []
                

                self.StartNewCons = 0
                # logger.info("Starting new average consensus with the initial value = " +str(self.myNum))
                # logger.info("self.consTol=" +str(self.consTol))
                time.sleep(0.1)
                
                         
            else:
                
                if (self.myAvgConsflag == 1):
                    # pass
                    
                    if (self.Avg_counter == 1):
                        # self.myMaxConsflag = 0
                        # logger.info(" I am here ")
                        pass
                    else:
                        # logger.info("Sending max convergence flag to primary = " +str(self.myMaxConsflag))
                        self.avg_cons_flag_to_primary(self.myAvgConsflag)
                        
                        n = np.sum(self.OutBufNums)                    
                        d = np.sum(self.OutBufDens) 
    
                        n = float(n)
                        d = float(d)
                        
                        M = self.myMax
                        M = float(M)
                        
                        m = self.myMin                    
                        m = float(m)

                        myUpdate = json.dumps({"numerator": n, "denominator": d, "max": M, "min": m, "iteration": self.iteration, "NbrID": self.myID})
     
                        self.broadcastMsgto_OUT_Nbrs(myUpdate)
                        
                        # self.receiveMsgfrom_IN_Nbrs()
                        # logger.info(" I am not here ")

                else: 
                    
                    if (self.myDen == 0):
                        self.iteration = 0
                    else:
                    
                        self.iteration += 1;      
                         
                        self.OutBufNums = np.concatenate((np.array(self.OutBufNums),np.multiply(self.myNum,weight)),axis=None)
                        self.OutBufDens = np.concatenate((np.array(self.OutBufDens),np.multiply(self.myDen,weight)),axis=None)
                        self.OutBufMaxs = np.concatenate((np.array(self.OutBufMaxs),self.myMax),axis=None)
                        self.OutBufMins = np.concatenate((np.array(self.OutBufMins),self.myMin),axis=None) 
                        
                        n = np.sum(self.OutBufNums)                    
                        d = np.sum(self.OutBufDens) 
    
                        n = float(n)
                        d = float(d)
                        
                        M = self.myMax
                        M = float(M)
                        
                        m = self.myMin                    
                        m = float(m)
                        
                
                        myUpdate = json.dumps({"numerator": n, "denominator": d, "max": M, "min": m, "iteration": self.iteration, "NbrID": self.myID})
     
                        self.broadcastMsgto_OUT_Nbrs(myUpdate)
                        
                        self.receiveMsgfrom_IN_Nbrs()
                        

                        tempNumSum = np.sum(self.OutBufNums)
                        tempDenSum = np.sum(self.OutBufDens)


                        self.myNum = (tempNumSum - self.myNumSumOld) + (np.sum(self.IN_neighborNumsSum) - self.myNbrNumSumOld) 
                        self.myDen = (tempDenSum - self.myDenSumOld) + (np.sum(self.IN_neighborDensSum) - self.myNbrDenSumOld)
                        
                        
                        self.myNumSumOld = tempNumSum
                        self.myDenSumOld = tempDenSum
                        self.myNbrNumSumOld = np.sum(self.IN_neighborNumsSum)
                        self.myNbrDenSumOld = np.sum(self.IN_neighborDensSum)
            
                        self.myRatio = np.divide(self.myNum, self.myDen)
                        
                        dummyMax = np.array(self.IN_neighborMaxs)
                        dummyMin = np.array(self.IN_neighborMins)                        
                        
                        
                        if (len(dummyMax) != 0):                
                            self.myMax = np.maximum(self.myMax, dummyMax.max())
                            self.myMin = np.minimum(self.myMin, dummyMin.min())
                        else:
                            self.myMax = self.myMax
                            self.myMin = self.myMin
                        
                            
                        if (np.mod(self.iteration, self.Diam) == 0):
                            
                            self.myMXP = self.myMax
                            self.myMNP = self.myMin
            
                            diff = np.abs(self.myMXP - self.myMNP)
                            # logger.info("my converged value = " + str(self.myRatio))
                            
                            # logger.info("diff = " +str(diff))
                            
                            if (diff < self.consTol):
                                # logger.info("seems like consensus is reached" + str(self.myRatio)) 
                                if (self.myMXP == 0 and self.myMNP == 0):
                                    pass
                                else:
                                    self.myAvgConsflag = 1
                                    self.avg_cons_flag_to_primary(self.myAvgConsflag)
                                
                                self.myMax = self.myRatio
                                self.myMin = self.myRatio
            
                            else:                
                                self.myMax = self.myRatio
                                self.myMin = self.myRatio
        
    
    
    def avg_cons_flag_to_primary(self, avg_cons_flag):
        avg_cons_flag = json.dumps(avg_cons_flag)
        self.connection_handler.send_message(avg_cons_flag, "avg_cons_flag")
        
    def max_cons_flag_to_primary(self, max_cons_flag):
        max_cons_flag = json.dumps(max_cons_flag)
        self.connection_handler.send_message(max_cons_flag, "max_cons_flag")
        
    def max_cons_value_to_primary(self, max_cons_value):
        max_cons_value = json.dumps(max_cons_value)
        self.connection_handler.send_message(max_cons_value, "max_cons_value")        
      
    def max_cons_init_flag_to_primary(self, max_cons_init_flag):
        max_cons_init_flag = json.dumps(max_cons_init_flag)
        # logger.info("Sending max_cons_init_flag to primary = " +str(max_cons_init_flag))
        self.connection_handler.send_message(max_cons_init_flag, "max_cons_init_flag")
    
    def max_cons_init_value_to_primary(self, max_cons_init_value):
        max_cons_init_value = json.dumps(max_cons_init_value)
        # logger.info("Sending max_cons_init_flag to primary = " +str(max_cons_init_flag))
        self.connection_handler.send_message(max_cons_init_value, "max_cons_init_value")
    
    def avg_cons_init_value_to_primary(self, avg_cons_init_value):
        avg_cons_init_value = json.dumps(avg_cons_init_value)
        # logger.info("Sending avg_cons_init_value to primary = " +str(avg_cons_init_value))
        self.connection_handler.send_message(avg_cons_init_value, "avg_cons_init_value")
        
    def avg_cons_init_flag_to_primary(self, avg_cons_init_flag):
        avg_cons_init_flag = json.dumps(avg_cons_init_flag)
        # logger.info("Sending avg_cons_init_flag to primary = " +str(avg_cons_init_flag))
        self.connection_handler.send_message(avg_cons_init_flag, "avg_cons_init_flag")
        
    def avg_cons_num_init_value_to_primary(self, avg_cons_num_init_value):
        avg_cons_num_init_value = json.dumps(avg_cons_num_init_value)
        # logger.info("Sending avg_cons_init_value to primary = " +str(avg_cons_init_value))
        self.connection_handler.send_message(avg_cons_num_init_value, "avg_cons_num_init_value")
        
    def avg_cons_num_init_flag_to_primary(self, avg_cons_num_init_flag):
        avg_cons_num_init_flag = json.dumps(avg_cons_num_init_flag)
        # logger.info("Sending avg_cons_init_flag to primary = " +str(avg_cons_init_flag))
        self.connection_handler.send_message(avg_cons_num_init_flag, "avg_cons_num_init_flag")    
        
    def avg_cons_den_init_value_to_primary(self, avg_cons_den_init_value):
        avg_cons_den_init_value = json.dumps(avg_cons_den_init_value)
        # logger.info("Sending avg_cons_den_init_value to primary = " +str(avg_cons_den_init_value))
        self.connection_handler.send_message(avg_cons_den_init_value, "avg_cons_den_init_value")
        
    def avg_cons_den_init_flag_to_primary(self, avg_cons_den_init_flag):
        avg_cons_den_init_flag = json.dumps(avg_cons_den_init_flag)
        # logger.info("Sending avg_cons_init_flag to primary = " +str(avg_cons_init_flag))
        self.connection_handler.send_message(avg_cons_den_init_flag, "avg_cons_den_init_flag")   
        
    def opt_complete_flag_to_primary(self, opt_complete_flag):
        opt_complete_flag = json.dumps(opt_complete_flag)
        self.connection_handler.send_message(opt_complete_flag, "opt_complete_flag")
        
    def opt_final_result_to_primary(self, opt_final_value):
        opt_final_value = json.dumps(opt_final_value) 
        # logger.info("Sending final converged value for setpoints to primary = " +str(opt_final_value))
        self.connection_handler.send_message(opt_final_value, "opt_final_value")    
     

