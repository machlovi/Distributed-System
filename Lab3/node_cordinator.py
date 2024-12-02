import os
import time
import xmlrpc.client
from xmlrpc.server import SimpleXMLRPCServer
import threading
import json
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG, 
                    format='%(asctime)s - Coordinator - %(levelname)s - %(message)s',
                    filename='./logs/coordinator_transactions.log')

# Custom transport class to support timeout
class TimeoutTransport(xmlrpc.client.Transport):
    def __init__(self, timeout=8):
        self.timeout = timeout
        super().__init__()

    def make_connection(self, host):
        connection = super().make_connection(host)
        connection.timeout = self.timeout  # Set the timeout on the connection
        return connection

def load_transaction_state():
    """Load the persisted transaction state from a file."""
    if os.path.exists('transaction_state.json'):
        with open('transaction_state.json', 'r') as f:
            state = json.load(f)
            logging.info(f"Transaction state loaded: {state}")
            return state
    return None


class CoordinatorNode:
    def __init__(self, node_id=1, port=8001):
        self.node_id = node_id
        self.port = port
        self.timeout = 8  # Timeout for each RPC call

        # # Participant configuration with explicit accounts
        # self.participants = {
        #     2: {'host': 'localhost', 'port': 8002, 'account': 'A'},
        #     3: {'host': 'localhost', 'port': 8003, 'account': 'B'}
        # }

        # Transaction state storage path
        self.transaction_log_path = './logs/transactions_log.json'
        self.transaction_state = {}

        # Load existing transaction state if available
        if os.path.exists(self.transaction_log_path):
            with open(self.transaction_log_path, 'r') as f:
                self.transaction_state = json.load(f)


                # Load the last known transaction state if available
        saved_state = load_transaction_state()

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
        self.server = SimpleXMLRPCServer(("localhost", port), allow_none=True)
        # self.server = QuietXMLRPCServer(("localhost", port), allow_none=True)

        self.server.register_function(self.start_transaction, "start_transaction")
        self.server.register_function(self.simulate_coordinator_crash, "simulate_coordinator_crash")
        self.server.register_function(self.recover_from_crash,"recover_from_crash")
    
    def _get_account_balance(self, account):
        """
        Helper method to get account balance
        """
        for node_id, node_info in self.participants.items():
            if node_info['account'] == account:
                try:
                    proxy = xmlrpc.client.ServerProxy(f"http://{node_info['host']}:{node_info['port']}/")
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
                    proxy = xmlrpc.client.ServerProxy(f"http://{node_info['host']}:{node_info['port']}/", transport=TimeoutTransport(self.timeout))
                    
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
                logging.info("All participants ready. Proceeding to commit.")
                print("All participants ready. Proceeding to commit.")
                
                # Commit phase
                commit_results = {}
                for node_id, node_info in self.participants.items():
                    try:
                        proxy = xmlrpc.client.ServerProxy(f"http://{node_info['host']}:{node_info['port']}/", transport=TimeoutTransport(self.timeout))

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
                        proxy = xmlrpc.client.ServerProxy(f"http://{node_info['host']}:{node_info['port']}/")
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
        

    def start_server(self):
        logging.info(f"Coordinator Node {self.node_id} starting on port {self.port}")
        print(f"Coordinator Node {self.node_id} starting on port {self.port}")
        self.server.serve_forever()

def main():
    # Create and start coordinator node
    coordinator = CoordinatorNode()
    coordinator.start_server()

if __name__ == "__main__":
    main()
