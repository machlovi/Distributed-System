import os
import time
import xmlrpc.client
from xmlrpc.server import SimpleXMLRPCServer
import json
import logging
import random

# Configure logging
logging.basicConfig(level=logging.DEBUG, 
                    format='%(asctime)s - Node 3 (Participant B) - %(levelname)s - %(message)s',
                    filename='participant_b_detailed.log')

class AccountManager:
    def __init__(self, account_name, initial_balance):
        self.account_name = account_name
        self.file_path = f"{account_name}_account.json"
        
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

class ParticipantNodeB:
    def __init__(self, node_id=3, port=8003, initial_balance=300):
        self.node_id = node_id
        self.port = port
        self.account_manager = AccountManager('B', initial_balance)
        
        # Server setup
        self.server = SimpleXMLRPCServer(("localhost", port), allow_none=True)
        self.server.register_function(self.prepare, "prepare")
        self.server.register_function(self.commit, "commit")
        self.server.register_function(self.abort, "abort")
    
    def prepare(self, transaction):
        """
        Prepare phase for transaction
        """
        logging.info(f"Prepare phase for transaction: {transaction}")
        print(f"Node B: Prepare phase for transaction: {transaction}")
        
        # Only validate if this is the destination account
        if transaction['destination_account'] != 'B':
            logging.info("Node B: Not destination account, prepared.")
            print("Node B: Not destination account, prepared.")
            return True
        
        logging.info(f"Node B: Prepared for destination account.")
        print("Node B: Prepared for destination account.")
        return True
    
    def commit(self, transaction):
        """
        Commit transaction
        """
        logging.info(f"Commit phase for transaction: {transaction}")
        print(f"Node B: Commit phase for transaction: {transaction}")
        
        # Only process if this is the destination account
        if transaction['destination_account'] != 'B':
            logging.info("Node B: Not destination account, committed.")
            print("Node B: Not destination account, committed.")
            return True
        
        # Process commit for Account B
        current_balance = self.account_manager.get_balance()
        new_balance = current_balance + transaction['amount']
        self.account_manager.update_balance(new_balance)
        
        logging.info(f"Node B: Committed. Old Balance: {current_balance}, New Balance: {new_balance}")
        print(f"Node B: Committed. Old Balance: {current_balance}, New Balance: {new_balance}")
        
        return True
    
    def abort(self, transaction):
        """
        Abort transaction
        """
        logging.info(f"Abort phase for transaction: {transaction}")
        print(f"Node B: Abort phase for transaction: {transaction}")
        return True
    
    def start_server(self):
        logging.info(f"Participant Node {self.node_id} (Account B) starting on port {self.port}")
        print(f"Participant Node {self.node_id} (Account B) starting on port {self.port}")
        self.server.serve_forever()

def main():
    # Create and start participant node for Account B
    participant = ParticipantNodeB()
    participant.start_server()

if __name__ == "__main__":
    main()