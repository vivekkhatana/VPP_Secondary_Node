import threading
from RpiCluster.RpiClusterExceptions import DisconnectionException
from RpiCluster.MainLogger import logger
from RpiCluster.ConnectionHandler import ConnectionHandler


class RpiClusterClient(threading.Thread):
    """This class is used to handle each secondary node that connects to the primary.

        When running this will continually get messages from the secondary node and return a response
        in some form.

        Attributes:
            uuid: A random UUID created for the node to give everyone a random ID
            primary: a reference to the primary to get information from it
            connection_handler: A handler that manages receiving and sending messages in the payload format
            address: Address of the client
            node_specifications: Details of the node if it provides it

    """

    def __init__(self, primary, clientsocket, address, nodeID, NetworkDiam): # agentNbhrConfig, new_influx_client
        threading.Thread.__init__(self)
        self.primary = primary
        self.connection_handler = ConnectionHandler(clientsocket)
        self.address = address
        self.nodeID = nodeID
        self.NetworkDiam = NetworkDiam
        self.sol = 0;
        self.avg_cons_conv_flag = 0;
        self.max_cons_conv_flag = 0;
        self.max_cons_init_conv_flag = 0;
        self.opt_complete_flag = 0;
        self.avg_cons_init_conv_flag = 0;
        self.avg_cons_init_value = 0;
        self.avg_cons_num_init_value = 0;
        self.avg_cons_num_init_conv_flag = 0;
        self.avg_cons_den_init_value = 0;
        self.avg_cons_den_init_conv_flag = 0;     
        self.max_cons_init_value = 0;
        self.max_cons_value = 0;
        
    def start(self):
        """Method that runs handling the secondary and serving all of its messages"""
        try:
            message = True
            while message:
                message = self.connection_handler.get_message()
                if message['type'] == 'message':
                    logger.info("Received message: " + message['payload'])
                elif message['type'] == 'opt_final_value':
                    self.sol = message['payload']
                elif message['type'] == 'avg_cons_flag':
                     self.avg_cons_conv_flag = message['payload']      
                elif message['type'] == 'max_cons_flag':
                     self.max_cons_conv_flag = message['payload']  
                elif message['type'] == 'max_cons_init_flag':
                     self.max_cons_init_conv_flag = message['payload'] 
                elif message['type'] == 'max_cons_init_value':
                     self.max_cons_init_value = message['payload']
                elif message['type'] == 'avg_cons_init_flag':
                     self.avg_cons_init_conv_flag = message['payload'] 
                elif message['type'] == 'avg_cons_init_value':
                     self.avg_cons_init_value = message['payload']
                elif message['type'] == 'avg_cons_num_init_flag':
                     self.avg_cons_num_init_conv_flag = message['payload'] 
                elif message['type'] == 'avg_cons_num_init_value':
                     self.avg_cons_num_init_value = message['payload']
                elif message['type'] == 'avg_cons_den_init_flag':
                     self.avg_cons_den_init_conv_flag = message['payload'] 
                elif message['type'] == 'avg_cons_den_init_value':
                     self.avg_cons_den_init_value = message['payload']
                elif message['type'] == 'max_cons_value':
                     self.max_cons_value = message['payload']
                elif message['type'] == 'opt_complete_flag':
                     self.opt_complete_flag = message['payload'] 
                elif message['type'] == 'info':
                    # logger.info("Secondary wants to know about its " + message['payload'])
                    if message['payload'] == 'nodeID':
                        self.connection_handler.send_message(self.nodeID, "nodeID")
                    elif message['payload'] == 'Diam':
                        self.connection_handler.send_message(self.NetworkDiam, "Diam")
                    elif message['payload'] == 'secondary_details':
                        secondary_details = self.primary.get_secondary_details()
                        self.connection_handler.send_message(secondary_details, "secondary_details")
                    else:
                        self.connection_handler.send_message("unknown", "bad_message")
        except DisconnectionException as e:
            logger.info("Got disconnection exception with message: " + e.message)
            logger.info("Shutting down secondary connection handler")
            self.primary.remove_client(self)
