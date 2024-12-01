import os
import time
import xmlrpc.client
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler

import threading
import json
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG, 
                    format='Coordinator - %(levelname)s - %(message)s',
                    filename='./logs/coordinator_transactions.log')


class QuietXMLRPCServer(SimpleXMLRPCServer):
    def __init__(self, *args, **kwargs):
        # Use the QuietXMLRPCRequestHandler to suppress logging
        kwargs['requestHandler'] = QuietXMLRPCRequestHandler
        super().__init__(*args, **kwargs)



class QuietXMLRPCRequestHandler(SimpleXMLRPCRequestHandler):
    def log_message(self, format, *args):
        # Override log_message to suppress all HTTP log messages
        pass

# Custom transport class to support timeout
class TimeoutTransport(xmlrpc.client.Transport):
    def __init__(self, timeout=8):
        self.timeout = timeout
        super().__init__()

    def make_connection(self, host):
        connection = super().make_connection(host)
        connection.timeout = self.timeout  # Set the timeout on the connection
        return connection


# Function to load the configuration from the config file
def load_config(config_file):
    """Load the configuration from a JSON file."""
    try:
        with open(config_file, 'r') as f:
            config_data = json.load(f)
            return config_data
    except FileNotFoundError:
        print(f"Error: Configuration file {config_file} not found.")
        return None
    except json.JSONDecodeError:
        print(f"Error: Failed to decode JSON from {config_file}.")
        return None


config = load_config('./config_file.json')
# print(config['participants'])
class CoordinatorNode:
 

    def __init__(self, node_id, ip_address, port, timeout, participants):
        self.node_id = node_id
        self.ip_address = ip_address
        self.port = port
        self.timeout = timeout  # Timeout for each RPC call
        self.participants = self.load_participants('./config_file.json')  # List of participant configurations
        self.transaction_log_path = './logs/transactions_log.json'
        self.transaction_state = {}
        self.recover = False


        # Load the last known transaction state if available
        saved_state = self.load_transaction_state()

        if saved_state:
            self.transaction_state = saved_state
            logging.info("Loaded previous transaction state. Resuming...")
            # Handle recovery logic
            if saved_state['status'] == 'prepared':
                logging.info("Recovering prepared transaction. Proceeding to commit.")
                self._commit_transaction(saved_state['transaction'])
        else:
            self.transaction_state = {}

        # Server setup
        # self.server = SimpleXMLRPCServer(("localhost", port), allow_none=True)
        self.server = QuietXMLRPCServer(("localhost", port), allow_none=True)

        self.server.register_function(self.start_transaction, "start_transaction")
        self.server.register_function(self.simulate_coordinator_crash, "simulate_coordinator_crash")
        self.server.register_function(self.recover_from_crash,"recover_from_crash")
  

        # Start the server

    def load_participants(self, config_file):
        """Load participants from a JSON configuration file"""
        try:
            with open(config_file, 'r') as file:
                config = json.load(file)
                participants = config['participants']
                # Convert list of participants into a dictionary using 'node_id' as the key
                participants_dict = {str(participant['node_id']): participant for participant in participants}

                return participants_dict # Return the dictionary of participants
        except FileNotFoundError:
            print(f"Error: Configuration file {config_file} not found.")
            return {}
        except json.JSONDecodeError:
            print(f"Error: Failed to decode JSON from {config_file}.")
            return {}


    def load_transaction_state(self):
        """Load the persisted transaction state from a file."""
        if os.path.exists(self.transaction_log_path):
            with open(self.transaction_log_path, 'r') as f:
                state = json.load(f)
                logging.info(f"Transaction state loaded: {state}")
                return state
        return None

    def simulate_coordinator_crash(self):
        """Simulate a coordinator crash by abruptly stopping."""
        logging.warning("Simulating coordinator crash...")
        os._exit(1)  # Abrupt exit without cleanup

    

    def recover_from_crash(self,recover=False):

        if not recover:
            logging.info("No recovery needed, proceeding with normal operation.")
            return True
        
        """Recover coordinator state after a crash."""
        logging.info("Recovering from crash...")
            
        # Load saved transaction state
        saved_state = self.load_transaction_state()
        print(saved_state)

        
        if saved_state:
            self.transaction_state = saved_state
            logging.info(f"Loaded previous transaction state: {self.transaction_state}")
            
            # Check the last transaction status
            if self.transaction_state['status'] == 'prepared':
                logging.info("Recovered a prepared transaction. Proceeding to commit.")
                return self._commit_transaction(self.transaction_state['transaction'])
            elif self.transaction_state['status'] == 'committed':
                logging.info("Transaction already committed. Resuming normal operations.")
                return True
            elif self.transaction_state['status'] == 'aborted':
                logging.info("Transaction already aborted. Resuming normal operations.")
                return True
        else:
            # No saved state, continue with normal operation
            self.transaction_state = {}
            logging.info("No previous state found. Resuming normal operation.")
        
        return True  # Return True to indicate that recovery completed successfully



    def _get_account_balance(self, account):
        """
        Helper method to get account balance
        """
        for node_id, node_info in self.participants.items():
            if node_info['account'] == account:
                try:
                    proxy = xmlrpc.client.ServerProxy(f"http://{node_info['ip_address']}:{node_info['port']}")
                    return proxy.get_balance()
                except Exception as e:
                    logging.error(f"Error getting balance for {account}: {e}")
                    return None
        return None
    
    def _save_transaction_state(self):
        """
        Save the current transaction state to a file
        """
        with open(self.transaction_log_path, 'w') as f:
            json.dump(self.transaction_state, f)


    def start_transaction(self, transaction):
        """
        Standard money transfer transaction
        """
        logging.info(f"Starting standard transaction: {transaction}")
        print(f"Coordinator: Starting standard transaction: {transaction}")

        

        # Check if 'recover' flag is passed in the transaction and call recover_from_crash accordingly
        if transaction.get('recover', False):
            self.recover_from_crash(recover=True)


        source_balance = self._get_account_balance(transaction['source_account'])
        if source_balance is None:
            logging.error("Unable to retrieve source account balance")
            return False
        
        # Check if sufficient funds
        if source_balance < transaction['amount']:
            logging.warning(f"Insufficient funds. Balance: {source_balance}, Required: {transaction['amount']}")
            return False
        
        # Prepare phase
        prepare_results = {}
        try:
            for node_id, node_info in self.participants.items():
                try:
                    proxy = xmlrpc.client.ServerProxy(f"http://{node_info['ip_address']}:{node_info['port']}", transport=TimeoutTransport(self.timeout))
                    
                    # Check if this node is involved in the transaction
                    if (transaction['source_account'] == node_info['account'] or 
                        transaction['destination_account'] == node_info['account']):
                        
                        prepare_result = proxy.prepare(transaction)
                        logging.info(f"Prepare result for Node {node_id}: {prepare_result}")
                        print(f"Prepare result for Node {node_id}: {prepare_result}")
                        
                        prepare_results[node_id] = prepare_result
                    else:
                        # If node is not involved, consider it prepared
                        prepare_results[node_id] = True
                
                except Exception as e:
                    logging.error(f"Error in prepare phase for Node {node_id}: {e}")
                    print(f"Error in prepare phase for Node {node_id}: {e}")
                    prepare_results[node_id] = False
            
            # Check if all participants are ready
            if all(prepare_results.values()):

                if transaction.get('simulate_crash', False):
                    # Simulate crash during prepare phase
                    logging.warning("Simulating crash during the prepare phase.")
                    self.simulate_coordinator_crash()
                    return False

                logging.info("All participants ready. Proceeding to commit.")
                print("All participants ready. Proceeding to commit.")
                
                # Commit phase
                commit_results = {}
                for node_id, node_info in self.participants.items():
                    try:
                        proxy = xmlrpc.client.ServerProxy(f"http://{node_info['ip_address']}:{node_info['port']}", transport=TimeoutTransport(self.timeout))

                        # Only commit for nodes involved in transaction
                        if (transaction['source_account'] == node_info['account'] or 
                            transaction['destination_account'] == node_info['account']):
                            
                            commit_result = proxy.commit(transaction)
                            logging.info(f"Commit result for Node {node_id}: {commit_result}")
                            print(f"Commit result for Node {node_id}: {commit_result}")
                            
                            commit_results[node_id] = commit_result
                        else:
                            # If node is not involved, consider it committed
                            commit_results[node_id] = True
                    
                    except Exception as e:
                        logging.error(f"Error in commit phase for Node {node_id}: {e}")
                        print(f"Error in commit phase for Node {node_id}: {e}")
                        commit_results[node_id] = False
                
                # Final transaction status
                transaction_success = all(commit_results.values())
                logging.info(f"Transaction final status: {transaction_success}")
                print(f"Transaction final status: {transaction_success}")
                
                # Save the transaction state for recovery
                self.transaction_state = {'transaction': transaction, 'status': 'committed' if transaction_success else 'aborted'}
                self._save_transaction_state()

                return transaction_success
            else:
                # Prepare phase failed, initiate abort
                logging.warning("Prepare phase failed. Initiating abort.")
                print("Prepare phase failed. Initiating abort.")
                
                for node_id, node_info in self.participants.items():
                    try:
                        proxy = xmlrpc.client.ServerProxy(f"http://{node_info['host']}:{node_info['port']}")
                        proxy.abort(transaction)
                    except Exception as e:
                        logging.error(f"Error during abort for Node {node_id}: {e}")
                
                self.transaction_state = {'transaction': transaction, 'status': 'aborted'}
                self._save_transaction_state()
                return False
        
  
        except Exception as e:
                    logging.critical(f"Unexpected error in transaction: {e}")
                    self.transaction_state = {'transaction': transaction, 'status': 'aborted'}
                    self._save_transaction_state()
                    return False
        

    # def start_server(self):
    #     logging.info(f"Coordinator Node {self.node_id} starting on port {self.port}")
    #     print(f"Coordinator Node {self.node_id} starting on port {self.port}")
    #     self.server.serve_forever()

    def start_server(self):
        # Log the starting message for the coordinator
        logging.info(f"Coordinator Node {self.node_id} starting on IP {self.ip_address} and port {self.port}")
        print(f"Coordinator Node {self.node_id} starting on IP {self.ip_address} and port {self.port}")
        self.server.serve_forever()

def main():
    # Create and start coordinator node
    # coordinator = CoordinatorNode()
    # coordinator.start_server()

       # Load configuration from the config file
    config = load_config('./config_file.json')
    if not config:
        return

    # Get the coordinator configuration
    coordinator_config = config["coordinator"]
    coordinator_node_id = coordinator_config["node_id"]
    coordinator_ip_address = coordinator_config["ip_address"]
    coordinator_port = coordinator_config["port"]
    coordinator_timeout = coordinator_config["timeout"]

    # Get participant configurations
    participants_config = config["participants"]



    # Start the coordinator node
    coordinator_node = CoordinatorNode(
        node_id=coordinator_node_id,
        ip_address=coordinator_ip_address,
        port=coordinator_port,
        timeout=coordinator_timeout,
        participants=participants_config
    )
    coordinator_node.start_server()

if __name__ == "__main__":
    main()
