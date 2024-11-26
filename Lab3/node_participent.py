import os
import time
import xmlrpc.client
from xmlrpc.server import SimpleXMLRPCServer
import json
import logging
import random
import argparse


# Configure logging
logging.basicConfig(level=logging.DEBUG, 
                    format='%(asctime)s - %(levelname)s - %(message)s')  # Simplified format


class AccountManager:
    def __init__(self, account_name, initial_balance):
        self.account_name = account_name
        self.file_path = f"./logs/{account_name}_account.json"
        
        # Initialize account file if it doesn't exist
        if not os.path.exists(self.file_path):
            with open(self.file_path, 'w') as f:
                json.dump({"balance": initial_balance}, f)
    
    def get_balance(self):
        with open(self.file_path, 'r') as f:
            return json.load(f)["balance"]
    
    def update_balance(self, new_balance):
        with open(self.file_path, 'w') as f:
            json.dump({"balance": new_balance}, f)


class ParticipantNode:

    def __init__(self, node_id, port, initial_balance):
        self.node_id = node_id
        self.port = port
        self.account_manager = AccountManager(f'node{node_id}', initial_balance)
        
        # Server setup
        self.server = SimpleXMLRPCServer(("localhost", port), allow_none=True)
        self.server.register_function(self.prepare, "prepare")
        self.server.register_function(self.commit, "commit")
        self.server.register_function(self.abort, "abort")
        self.server.register_function(self.set_initial_balance, "set_initial_balance")  # Register this method
        self.server.register_function(self.get_balance, "get_balance")

    def set_initial_balance(self, initial_balance):
        """Set the initial balance for the account."""
        self.account_manager.update_balance(initial_balance)
        logging.info(f"Initial balance set to: {initial_balance}")
        return f"Initial balance set to: {initial_balance}"
    
    def get_balance(self):
        """
        Expose balance retrieval
        """
        current_balance = self.account_manager.get_balance()
        logging.info(f"Balance request for Account A: {current_balance}")
        print(f"Node A: Balance request: {current_balance}")
        return current_balance
    
    def prepare(self, transaction):
        """
        Prepare phase for transaction
        """
        logging.info(f"Prepare phase for transaction: {transaction}")
        print(f"Node {self.node_id}: Prepare phase for transaction: {transaction}")

        # Use 'A' or 'B' for account identification
        if transaction['source_account'] != chr(64 + self.node_id):  # 'A' for node 2, 'B' for node 3
            logging.info(f"Node {self.node_id}: Not source account, prepared.")
            print(f"Node {self.node_id}: Not source account, prepared.")
            return True
        
        # Validate transaction
        amount = transaction['amount']
        current_balance = self.account_manager.get_balance()
        
        is_prepared = current_balance >= amount
        logging.info(f"Node {self.node_id}: Prepare result. Balance: {current_balance}, Amount: {amount}, Prepared: {is_prepared}")
        print(f"Node {self.node_id}: Prepare result. Balance: {current_balance}, Amount: {amount}, Prepared: {is_prepared}")
        
        return is_prepared
    
    def commit(self, transaction):
        """
        Commit transaction
        """
        logging.info(f"Commit phase for transaction: {transaction}")
        print(f"Node {self.node_id}: Commit phase for transaction: {transaction}")

        # Process for the source account (deduct funds)
        if transaction['source_account'] == chr(64 + self.node_id):  # 'A' for node 2, 'B' for node 3
            current_balance = self.account_manager.get_balance()
            new_balance = current_balance - transaction['amount']
            self.account_manager.update_balance(new_balance)
            
            logging.info(f"Node {self.node_id}: Committed. Old Balance: {current_balance}, New Balance: {new_balance}")
            print(f"Node {self.node_id}: Committed. Old Balance: {current_balance}, New Balance: {new_balance}")
        
        # Process for the destination account (add funds)
        elif transaction['destination_account'] == chr(64 + self.node_id):  # 'A' for node 2, 'B' for node 3
            current_balance = self.account_manager.get_balance()
            new_balance = current_balance + transaction['amount']
            self.account_manager.update_balance(new_balance)
            
            logging.info(f"Node {self.node_id}: Committed. Old Balance: {current_balance}, New Balance: {new_balance}")
            print(f"Node {self.node_id}: Committed. Old Balance: {current_balance}, New Balance: {new_balance}")
        
        return True

    def abort(self, transaction):
        """
        Abort transaction
        """
        logging.info(f"Abort phase for transaction: {transaction}")
        print(f"Node {self.node_id}: Abort phase for transaction: {transaction}")
        return True
    
    def start_server(self):
        logging.info(f"Participant Node {self.node_id} (Account A) starting on port {self.port}")
        print(f"Participant Node {self.node_id} (Account A) starting on port {self.port}")
        self.server.serve_forever()


def main():
    parser = argparse.ArgumentParser(description="Start Participant Node.")
    parser.add_argument("node", type=str, choices=["node2", "node3"], help="Node to start (node2 or node3)")
    args = parser.parse_args()

    if args.node == "node2":
        node_id = 2
        port = 8002
        initial_balance = 100  # Example initial balance
        participant = 'A'
    elif args.node == "node3":
        node_id = 3
        port = 8003
        initial_balance = 100  # Example initial balance
        participant = 'B'

    logging.basicConfig(level=logging.DEBUG, 
                        format='%(asctime)s - %(levelname)s - %(message)s',  # Simplified format
                        filename=f'./logs/{participant}_participant_detailed.log')
    
    participant_node = ParticipantNode(node_id=node_id, port=port, initial_balance=initial_balance)
    participant_node.start_server()

if __name__ == "__main__":
    main()
