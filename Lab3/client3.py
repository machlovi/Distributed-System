import xmlrpc.client
import time
import logging
import json
import os

# Ensure logs directory exists
if not os.path.exists('./logs'):
    os.makedirs('./logs')

# Configure logging
logging.basicConfig(
    level=logging.DEBUG, 
    format='%(asctime)s - Client - %(levelname)s - %(message)s',
    filename='./logs/client_detailed.log'
)

# Load the configuration
def load_config(config_file):
    """Load the configuration from a JSON file."""
    try:
        with open(config_file, 'r') as f:
            return json.load(f)
    except Exception as e:
        logging.error(f"Error loading config file: {e}")
        return None
    
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

def simulate_coordinator_crash():
    """Simulate a coordinator crash by calling the crash endpoint."""
    try:
        coordinator = xmlrpc.client.ServerProxy(coordinator_url)
        logging.info("Simulating coordinator crash.")
        coordinator.simulate_coordinator_crash()
    except Exception as e:
        logging.info(f"Coordinator crash simulation completed: {e}")

def recover_coordinator(recover):
    """Call the coordinator to recover from a crash."""
    try:
        coordinator = xmlrpc.client.ServerProxy(coordinator_url)
        logging.info("Recovering coordinator.")
        result = coordinator.recover_from_crash(recover)
        logging.info(f"Coordinator recovery result: {result}")
        print(f"Client: Coordinator recovery result: {result}")
    except Exception as e:
        logging.error(f"Error recovering coordinator: {e}")
        print(f"Client: Error recovering coordinator: {e}")

def set_crash_scenario(proxy, scenario):
    """Set the crash scenario for a participant node."""
    return proxy.set_crash_scenario(scenario)

def start_participant_node_with_balance(config, participant, initial_balance, scenario=None):
    """
    Start a participant node with an initial balance and optional crash scenario.
    """
    participant_ip = participant['ip_address']
    participant_port = participant['port']
    # print(f'http://{participant_ip}:{participant_port}')
    
    try:
        with xmlrpc.client.ServerProxy(f'http://{participant_ip}:{participant_port}') as proxy:
            response = proxy.set_initial_balance(initial_balance)
            print(f"Participant Node {participant_port}: {response}")
            
            if scenario:
                response = set_crash_scenario(proxy, scenario)
                print(f"Participant Node {participant_port}: Crash scenario set to {scenario}.")
    except Exception as e:
        logging.error(f"Error starting participant node: {e}")
        print(f"Client: Error starting participant node: {e}")

def main():
    """
    Main function to run transaction scenarios, including crashes and recovery.
    """
    # Start participant nodes with initial balances
    print("\nStarting participant nodes with initial balances.")
    for participant in config['participants']:
        start_participant_node_with_balance(config, participant, participant['initial_balance'])

    print(
        """
        ************************************************************************
                     Scenario 1: Successful Transaction
        ************************************************************************
        """
    )
    initiate_transaction('A', 'B', 100)
    time.sleep(5)

    print(
    """
    ************************************************************************
                    Scenario 2: Insufficient Funds
    ************************************************************************
    """)
    start_participant_node_with_balance(config, config['participants'][0], 90)  # Node 1 with insufficient funds
    start_participant_node_with_balance(config, config['participants'][1], 50)  # Node 2 with insufficient funds
    initiate_transaction('A', 'B', 100)
    time.sleep(5)

    print(
    """
    ************************************************************************
                    Scenario 3: Crash before response Node 2
    ************************************************************************
    """)
    # Set crash scenario for Node-2 before response
    start_participant_node_with_balance(config, config['participants'][0], 200, scenario='before_response') 
    start_participant_node_with_balance(config, config['participants'][1], 300)
    initiate_transaction('A', 'B', 100)
    
    print(
    """
    ************************************************************************
                    Scenario 4: Crash after response Node 2
    ************************************************************************
    """)
    start_participant_node_with_balance(config, config['participants'][0], 200, scenario='after_response')
    initiate_transaction('A', 'B', 100)
    time.sleep(2)

    print(
        """
        ************************************************************************
                     Scenario 5: Coordinator Crash During Transaction
        ************************************************************************
        """
    )
    initiate_transaction('A', 'B', 100, simulate_crash=True)
    time.sleep(15)

    # Recover the coordinator after the crash
    print("Client: Recovering coordinator...")
    recover_coordinator(recover=True)
    time.sleep(5)

if __name__ == "__main__":
    main()
