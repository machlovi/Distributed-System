import os
import time
import xmlrpc.client
import json
import logging
import random
import argparse
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler

import threading


class QuietXMLRPCServer(SimpleXMLRPCServer):
    def __init__(self, *args, **kwargs):
        # Use the QuietXMLRPCRequestHandler to suppress logging
        kwargs['requestHandler'] = QuietXMLRPCRequestHandler
        super().__init__(*args, **kwargs)



class QuietXMLRPCRequestHandler(SimpleXMLRPCRequestHandler):
    def log_message(self, format, *args):
        # Override log_message to suppress all HTTP log messages
        pass

logging.basicConfig(level=logging.DEBUG, 
                    format=' %(levelname)s - %(message)s',  # Simplified format
                    filename='./logs/participant_detailed.log')

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


    def __init__(self, node_id, ip_address, port, initial_balance,crash_scenario=None):
        self.node_id = node_id
        self.ip_address = ip_address
        self.port = port
        self.account_manager = AccountManager(f'node{node_id}', initial_balance)
        self.crash_scenario = crash_scenario  # Added crash scenario


        # Set up the server
        # self.server = SimpleXMLRPCServer((self.ip_address, self.port), allow_none=True)
        self.server = QuietXMLRPCServer(("0.0.0.0", port), allow_none=True)


        
 
        # self.server = QuietXMLRPCServer(("localhost", port), allow_none=True)




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
        

        if self.crash_scenario == 'before_response' and self.node_id == 7:
            logging.info(f"Simulating crash for Node {self.node_id} before responding...")
            time.sleep(15)  # Simulate long delay (crash)


        # Use 'A' or 'B' for account identification

        else:
            transaction['source_account'] != chr(64 + self.node_id)  # 'A' for node 2, 'B' for node 3

            logging.info(f"Node {self.node_id}: Not source account, prepared.")
            # print(f"Node {self.node_id}: Not source account, prepared.")
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
        if self.crash_scenario == 'after_response' and self.node_id == 7:
            logging.info(f"Simulating crash for Node {self.node_id} after responding...")
            time.sleep(15)  # Simulate long delay (crash)
            # return False  # Node-2 fails to commit

        try:
            # Step 1: Transfer funds from source account
            if transaction['source_account'] == 'A' and self.node_id == 7:  # Node 7 is Account A
                current_balance_A = self.account_manager.get_balance()
                if current_balance_A < transaction['amount']:
                    logging.error("Insufficient funds for transaction")
                    return False
                
                new_balance_A = current_balance_A - transaction['amount']
                self.account_manager.update_balance(new_balance_A)
                
                logging.info(f"Node A (7): Funds transferred. Old Balance: {current_balance_A}, New Balance: {new_balance_A}")
                print(f"Node A (7): Funds transferred. Old Balance: {current_balance_A}, New Balance: {new_balance_A}")

            elif transaction['source_account'] == 'B' and self.node_id == 8:  # Node 8 is Account B
                current_balance_B = self.account_manager.get_balance()
                if current_balance_B < transaction['amount']:
                    logging.error("Insufficient funds for transaction")
                    return False
                
                new_balance_B = current_balance_B - transaction['amount']
                self.account_manager.update_balance(new_balance_B)

                logging.info(f"Node B (8): Funds transferred. Old Balance: {current_balance_B}, New Balance: {new_balance_B}")
                print(f"Node B (8): Funds transferred. Old Balance: {current_balance_B}, New Balance: {new_balance_B}")

            # Step 2: Add funds to the destination account
            if transaction['destination_account'] == 'B' and self.node_id == 8:  # Node 8 is Account B
                current_balance_B = self.account_manager.get_balance()
                new_balance_B = current_balance_B + transaction['amount']
                self.account_manager.update_balance(new_balance_B)
                
                logging.info(f"Node B (8): Funds received. Old Balance: {current_balance_B}, New Balance: {new_balance_B}")
                print(f"Node B (8): Funds received. Old Balance: {current_balance_B}, New Balance: {new_balance_B}")
            
            elif transaction['destination_account'] == 'A' and self.node_id == 7:  # Node 7 is Account A
                current_balance_A = self.account_manager.get_balance()
                new_balance_A = current_balance_A + transaction['amount']
                self.account_manager.update_balance(new_balance_A)

                logging.info(f"Node A (7): Funds received. Old Balance: {current_balance_A}, New Balance: {new_balance_A}")
                print(f"Node A (7): Funds received. Old Balance: {current_balance_A}, New Balance: {new_balance_A}")
            
            # Step 3: Apply bonus (20%) to both A and B
            if self.node_id == 7:  # Node 2 is 'A'
                current_balance_A = self.account_manager.get_balance()
                bonus_A = 0.2 * current_balance_A  # 20% bonus
                new_balance_A = current_balance_A + bonus_A
                self.account_manager.update_balance(new_balance_A)
                
                logging.info(f"Node A (7): Bonus added. Old Balance: {current_balance_A}, New Balance: {new_balance_A}")
                print(f"Node A (7): Bonus added. Old Balance: {current_balance_A}, New Balance: {new_balance_A}")
            
            elif self.node_id == 8:  # Node 3 is 'B'
                current_balance_B = self.account_manager.get_balance()
                bonus_B = 0.2 * current_balance_B  # 20% bonus
                new_balance_B = current_balance_B + bonus_B
                self.account_manager.update_balance(new_balance_B)
                
                logging.info(f"Node B (8): Bonus added. Old Balance: {current_balance_B}, New Balance: {new_balance_B}")
                print(f"Node B (8): Bonus added. Old Balance: {current_balance_B}, New Balance: {new_balance_B}")
            
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
        account_label = 'A' if self.node_id == 7 else 'B'

        # Log the starting message with correct account label
        logging.info(f"Participant Node {self.node_id} Account {account_label} starting on port {self.port}")
        print(f"Participant Node {self.node_id} Accou {account_label} starting on port {self.port}")

        # Start the server
        self.server.serve_forever()


def find_leader(cluster,current_leader=None):
    config = load_config('./config_file.json')
    # print("leader")
    """Attempts to find the leader by querying each node, and reprints the leader if it changes."""
    # for node_info in config['clusterA'].values():  # Assuming config contains the nodes as a list of lists
    for node_info in config[cluster].values():  # Assuming config contains the nodes as a list of lists
        print(node_info)

        # Construct the URL string from node_info (['localhost', 8001] -> 'http://localhost:8001')
        node_url = f"http://{node_info[0]}:{node_info[1]}"
        
        try:
            with xmlrpc.client.ServerProxy(node_url) as client:
                
                if client.is_leader():
                    # If the leader has changed, notify the user
                    print(f"leader found at: {node_url}")
                    if node_url != current_leader:
                        logging.info(f"Leader changed: New leader found at {node_url}")
                    else:
                        logging.info(f"Leader found at {node_url}")
                    return node_url
        except Exception as e:
            logging.warning(f"Failed to connect to {node_url}: {e}")
    
    logging.error("Leader not found after checking all nodes.")
    return None

def write_value_to_leader(leader_url, value, simulate_failure=False):
    """Submit values to the current leader and handle leader failure scenarios."""
    if leader_url:
        try:
            logging.info(f"Attempting to write value to leader at {leader_url}")
            with xmlrpc.client.ServerProxy(leader_url) as client:
                # Get the leader's current balance (if needed) or any other action
                # leader_balance = client.get_balance()  # You may want to fetch the balance before submitting
                # logging.info(f"Leader current balance: {leader_balance}")
                
                
                # Simulate crash or failure if requested
                if simulate_failure:
                    client.set_replication_simulation(simulate_failure)
                
                # # Submit values
                response = client.submit_value(value)
                logging.info(f"Response from leader: {response}")
                print(response)
                
                if "Error" in response:
                    logging.warning("Error submitting value, attempting to find new leader.")
                    return find_leader(leader_url)  # Retry finding the leader if submission fails
                else:
                    logging.info(f"Transaction submitted successfully to leader at {leader_url}")
                    return True  # Return True when the leader successfully processes the transaction

        except Exception as e:
            logging.error(f"Failed to submit value to leader at {leader_url}: {e}")
            # Retry leader detection if submission to the leader fails
            return find_leader(leader_url)
    else:
        logging.warning("No leader found to write to.")
        return leader_url  # If no leader is found, return the leader_url


def start_server_in_thread(participant_node):
    """Start the participant node's server in a separate thread."""
    server_thread = threading.Thread(target=participant_node.start_server)
    server_thread.daemon = True  # Allow the program to exit even if the thread is running
    server_thread.start()
    return server_thread


def submit_values_with_leader_detection(cluster,current_balance):
    """Main loop for user interactions with the Raft cluster."""
    leader_url = find_leader(cluster)  # Initial leader detection
    print("submiting values")

    
    # Submit values to the leader
    leader_url = write_value_to_leader(leader_url,current_balance, simulate_failure=False)
    if leader_url:
        logging.info(f"Leader is at {leader_url}")
        return True
    else:
        logging.warning("No leader found to write to.")
        return  # Exit if no leader is found








config = load_config('./config_file.json')  # Updated file path
coordinator_config = config['coordinator']
coordinator_url = f"http://{coordinator_config['ip_address']}:{coordinator_config['port']}"

def initiate_transaction(source, destination, amount, simulate_crash=False):
    """
    Client transaction initiator with detailed logging.
    Initiates a transaction request to the coordinator.
    """
    try:
        # Create a proxy to the coordinator
        coordinator = xmlrpc.client.ServerProxy(coordinator_url)
        
        # Prepare transaction details
        transaction = {
            'source_account': source,
            'destination_account': destination,
            'amount': amount,
            'simulate_crash': simulate_crash
        }
        logging.info(f"Initiating transaction: {source} -> {destination}, Amount: {amount}")
        print(f"Client: Initiating transaction: {source} -> {destination}, Amount: {amount}")
        
        # Start the transaction through the coordinator
        result = coordinator.start_transaction(transaction)
        
        logging.info(f"Transaction result: {result}")
        print(f"Client: Transaction result: {result}")
        return result
    
    except Exception as e:
        logging.error(f"Error initiating transaction: {e}")
        print(f"Client: Error initiating transaction: {e}")
        return False
    

def set_crash_scenario(proxy, scenario):
    """Set the crash scenario for a participant node."""
    return proxy.set_crash_scenario(scenario)

def start_participant_node_with_balance(participant, initial_balance, scenario=None):
    """
    Start a participant node with an initial balance and optional crash scenario.
    """
    participant_ip = participant['ip_address']
    participant_port = participant['port']
    print(f'http://{participant_ip}:{participant_port}')
    
    try:
        with xmlrpc.client.ServerProxy(f'http://{participant_ip}:{participant_port}') as proxy:
            response = proxy.set_initial_balance(initial_balance)
            print(f"Participant Node {participant_port}: {response}")
            
           
    except Exception as e:
        logging.error(f"Error starting participant node: {e}")
        print(f"Client: Error starting participant node: {e}")




def main():
    parser = argparse.ArgumentParser(description="Start Participant Node.")
    parser.add_argument("node", type=str, choices=["node7", "node8"], help="Node to start (node7 or node8)")
    args = parser.parse_args()

    # Load configuration from the config file
    config = load_config('config_file.json')

    if not config:
        return



        # Determine the correct cluster based on the node selected
    if args.node == "node7" or args.node == "node8":  # Modify if you have more nodes
        if args.node == "node7":
            cluster = "clusterA"
        else:
            cluster = "clusterB"

        # Get participant configurations from the loaded config
        participants_config = config["participants"]


    # Look for the specific node requested
    for participant_config in participants_config:
        node_id = participant_config["node_id"]
        if args.node == f"node{node_id}":
            ip_address = participant_config["ip_address"]
            port = participant_config["port"]
            initial_balance = participant_config["initial_balance"]
            account = participant_config["account"]

            # Set up logging for each participant
            log_dir = './logs'
            if not os.path.exists(log_dir):
                os.makedirs(log_dir)

            log_filename = f'{log_dir}/{account}_participant_detailed.log'
            logging.basicConfig(level=logging.DEBUG,
                                format='%(levelname)s - %(message)s')

            logging.info(f"Starting Participant Node {node_id} - {account}")

            # Start the participant node
            participant_node = ParticipantNode(
                node_id=node_id,
                ip_address=ip_address,
                port=port,
                initial_balance=initial_balance
            )

            # Start the participant's server in a separate thread
            server_thread = start_server_in_thread(participant_node)

            # Now that the server is running, you can interact with it
            print("Submitting values and getting balance...")
            # submit_values_with_leader_detection()
                 # Ensure we only initiate balance update for the running node
                 
            if args.node == f"node7":
                # Update balance for the respective running node
                start_participant_node_with_balance(participant_config, initial_balance=200)
            else:
                start_participant_node_with_balance(participant_config, initial_balance=300)



            # Get the balance of the participant node after the transaction
            try:
                with xmlrpc.client.ServerProxy(f"http://{ip_address}:{port}") as proxy:
                  
                    current_balance = proxy.get_balance()
                    print(current_balance)
                    value=submit_values_with_leader_detection(cluster,current_balance)
                    print(value)
        
                    
                    logging.info(f"Balance for Participant {account}: {current_balance}")
                    print(f"Balance for Participant {account}: {current_balance}")
                    if value and args.node == "node7":
                        initiate_transaction('A', 'B', 100)
                            
                    # else:
                    #     logging.error("Nothing to do.")

                    current_balance = proxy.get_balance()
                    print(f"updating balace {current_balance}")
                    value=submit_values_with_leader_detection(cluster,current_balance)

            except Exception as e:
                logging.error(f"Error getting balance from participant node: {e}")

            # Continue with other operations
            server_thread.join()  # Wait for the server thread to finish if needed
            break  # Exit after the selected node is started


if __name__ == "__main__":
    main()
