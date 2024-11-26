import os
import time
import xmlrpc.client
from xmlrpc.server import SimpleXMLRPCServer
import json
import logging
import random
import argparse


# # Configure logging
# logging.basicConfig(level=logging.DEBUG, 
#                     format='%(asctime)s - %(levelname)s - %(message)s')  # Simplified format


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

    def __init__(self, node_id, port, initial_balance,crash_scenario=None):
        self.node_id = node_id
        self.port = port
        self.account_manager = AccountManager(f'node{node_id}', initial_balance)
        self.crash_scenario = crash_scenario  # Added crash scenario

        
        # Server setup
        self.server = SimpleXMLRPCServer(("localhost", port), allow_none=True)
        # self.server.register_instance(ParticipantNode())

        self.server.register_function(self.prepare, "prepare")
        self.server.register_function(self.commit, "commit")
        self.server.register_function(self.abort, "abort")
        self.server.register_function(self.set_initial_balance, "set_initial_balance")  # Register this method
        self.server.register_function(self.get_balance, "get_balance")
        self.server.register_function(self.set_crash_scenario,"set_crash_scenario")


    def set_initial_balance(self, initial_balance):
        """Set the initial balance for the account."""
        self.account_manager.update_balance(initial_balance)
        logging.info(f"Initial balance set to: {initial_balance}")
        return f"Initial balance set to: {initial_balance}"


    def set_crash_scenario(self, scenario):
        """Set the crash scenario to simulate"""
        self.crash_scenario = scenario
        logging.info(f"Crash scenario set to: {scenario}")
        return f"Crash scenario set to: {scenario}"
    
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
        




        if self.crash_scenario == 'before_response' and self.node_id == 2:
            logging.info(f"Simulating crash for Node {self.node_id} before responding...")
            time.sleep(15)  # Simulate long delay (crash)


        # Use 'A' or 'B' for account identification

        else:
            transaction['source_account'] != chr(64 + self.node_id)  # 'A' for node 2, 'B' for node 3

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

        # Simulate crash after responding in the 'commit' phase for Node-2
        if self.crash_scenario == 'after_response' and self.node_id == 2:
            logging.info(f"Simulating crash for Node {self.node_id} after responding...")
            time.sleep(15)  # Simulate long delay (crash)
            # return False  # Node-2 fails to commit

        try:
            # Step 1: Transfer funds from source account
            if transaction['source_account'] == 'A' and self.node_id == 2:  # Node 2 is Account A
                current_balance_A = self.account_manager.get_balance()
                if current_balance_A < transaction['amount']:
                    logging.error("Insufficient funds for transaction")
                    return False
                
                new_balance_A = current_balance_A - transaction['amount']
                self.account_manager.update_balance(new_balance_A)
                
                logging.info(f"Node A (2): Funds transferred. Old Balance: {current_balance_A}, New Balance: {new_balance_A}")
                print(f"Node A (2): Funds transferred. Old Balance: {current_balance_A}, New Balance: {new_balance_A}")

            elif transaction['source_account'] == 'B' and self.node_id == 3:  # Node 3 is Account B
                current_balance_B = self.account_manager.get_balance()
                if current_balance_B < transaction['amount']:
                    logging.error("Insufficient funds for transaction")
                    return False
                
                new_balance_B = current_balance_B - transaction['amount']
                self.account_manager.update_balance(new_balance_B)

                logging.info(f"Node B (3): Funds transferred. Old Balance: {current_balance_B}, New Balance: {new_balance_B}")
                print(f"Node B (3): Funds transferred. Old Balance: {current_balance_B}, New Balance: {new_balance_B}")

            # Step 2: Add funds to the destination account
            if transaction['destination_account'] == 'B' and self.node_id == 3:  # Node 3 is Account B
                current_balance_B = self.account_manager.get_balance()
                new_balance_B = current_balance_B + transaction['amount']
                self.account_manager.update_balance(new_balance_B)
                
                logging.info(f"Node B (3): Funds received. Old Balance: {current_balance_B}, New Balance: {new_balance_B}")
                print(f"Node B (3): Funds received. Old Balance: {current_balance_B}, New Balance: {new_balance_B}")
            
            elif transaction['destination_account'] == 'A' and self.node_id == 2:  # Node 2 is Account A
                current_balance_A = self.account_manager.get_balance()
                new_balance_A = current_balance_A + transaction['amount']
                self.account_manager.update_balance(new_balance_A)

                logging.info(f"Node A (2): Funds received. Old Balance: {current_balance_A}, New Balance: {new_balance_A}")
                print(f"Node A (2): Funds received. Old Balance: {current_balance_A}, New Balance: {new_balance_A}")
            
            # Step 3: Apply bonus (20%) to both A and B
            if self.node_id == 2:  # Node 2 is 'A'
                current_balance_A = self.account_manager.get_balance()
                bonus_A = 0.2 * current_balance_A  # 20% bonus
                new_balance_A = current_balance_A + bonus_A
                self.account_manager.update_balance(new_balance_A)
                
                logging.info(f"Node A (2): Bonus added. Old Balance: {current_balance_A}, New Balance: {new_balance_A}")
                print(f"Node A (2): Bonus added. Old Balance: {current_balance_A}, New Balance: {new_balance_A}")
            
            elif self.node_id == 3:  # Node 3 is 'B'
                current_balance_B = self.account_manager.get_balance()
                bonus_B = 0.2 * current_balance_B  # 20% bonus
                new_balance_B = current_balance_B + bonus_B
                self.account_manager.update_balance(new_balance_B)
                
                logging.info(f"Node B (3): Bonus added. Old Balance: {current_balance_B}, New Balance: {new_balance_B}")
                print(f"Node B (3): Bonus added. Old Balance: {current_balance_B}, New Balance: {new_balance_B}")
            
            return True

        except Exception as e:
            logging.error(f"Transaction failed: {e}")
            return False

    

    def abort(self, transaction):
        """
        Abort transaction
        """
        logging.info(f"Abort phase for transaction: {transaction}")
        print(f"Node {self.node_id}: Abort phase for transaction: {transaction}")
        return True
    
    def start_server(self):
        # Assign account label based on node ID
        account_label = 'A' if self.node_id == 2 else 'B'

        # Log the starting message with correct account label
        logging.info(f"Participant Node {self.node_id} Account {account_label} starting on port {self.port}")
        print(f"Participant Node {self.node_id} Account {account_label} starting on port {self.port}")

        # Start the server
        self.server.serve_forever()


def main():
    parser = argparse.ArgumentParser(description="Start Participant Node.")
    parser.add_argument("node", type=str, choices=["node2", "node3"], help="Node to start (node2 or node3)")
    args = parser.parse_args()

    if args.node == "node2":
        node_id = 2
        port = 8002
        initial_balance = 0  # Example initial balance
        participant = 'A'
        
    elif args.node == "node3":
        node_id = 3
        port = 8003
        initial_balance = 0  # Example initial balance
        participant = 'B'

    if not os.path.exists('./logs'):
        os.makedirs('./logs')

    # Set up logging for each participant with unique log files
    logging.basicConfig(level=logging.DEBUG, 
                        format='%(asctime)s - %(levelname)s - %(message)s',  
                        filename=f'./logs/{participant}_participant_detailed.log')
    
    participant_node = ParticipantNode(node_id=node_id, port=port, initial_balance=initial_balance)
    participant_node.start_server()


    

if __name__ == "__main__":
    main()
