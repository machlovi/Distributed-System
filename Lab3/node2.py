import os
import time
import xmlrpc.client
from xmlrpc.server import SimpleXMLRPCServer
import json
import logging
import random

# Configure logging
logging.basicConfig(level=logging.DEBUG, 
                    format='%(asctime)s - Node 2 (Participant A) - %(levelname)s - %(message)s',
                    filename='participant_a_detailed.log')

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

class ParticipantNodeA:
    def __init__(self, node_id=2, port=8002, initial_balance=200):
        self.node_id = node_id
        self.port = port
        self.account_manager = AccountManager('A', initial_balance)
        
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
        print(f"Node A: Prepare phase for transaction: {transaction}")
        
        # Only validate if this is the source account
        if transaction['source_account'] != 'A':
            logging.info("Node A: Not source account, prepared.")
            print("Node A: Not source account, prepared.")
            return True
        
        # Validate transaction
        amount = transaction['amount']
        current_balance = self.account_manager.get_balance()
        
        is_prepared = current_balance >= amount
        logging.info(f"Node A: Prepare result. Balance: {current_balance}, Amount: {amount}, Prepared: {is_prepared}")
        print(f"Node A: Prepare result. Balance: {current_balance}, Amount: {amount}, Prepared: {is_prepared}")
        
        return is_prepared
    
    def commit(self, transaction):
        """
        Commit transaction
        """
        logging.info(f"Commit phase for transaction: {transaction}")
        print(f"Node A: Commit phase for transaction: {transaction}")
        
        # Only process if this is the source account
        if transaction['source_account'] != 'A':
            logging.info("Node A: Not source account, committed.")
            print("Node A: Not source account, committed.")
            return True
        
        # Process commit for Account A
        current_balance = self.account_manager.get_balance()
        new_balance = current_balance - transaction['amount']
        self.account_manager.update_balance(new_balance)
        
        logging.info(f"Node A: Committed. Old Balance: {current_balance}, New Balance: {new_balance}")
        print(f"Node A: Committed. Old Balance: {current_balance}, New Balance: {new_balance}")
        
        return True
    
    def abort(self, transaction):
        """
        Abort transaction
        """
        logging.info(f"Abort phase for transaction: {transaction}")
        print(f"Node A: Abort phase for transaction: {transaction}")
        return True
    
    def start_server(self):
        logging.info(f"Participant Node {self.node_id} (Account A) starting on port {self.port}")
        print(f"Participant Node {self.node_id} (Account A) starting on port {self.port}")
        self.server.serve_forever()

def main():
    # Create and start participant node for Account A
    participant = ParticipantNodeA()
    participant.start_server()

if __name__ == "__main__":
    main()