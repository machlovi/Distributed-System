import xmlrpc.client
import time
import logging

# Configure logging
logging.basicConfig(
    level=logging.DEBUG, 
    format='%(asctime)s - Client - %(levelname)s - %(message)s',
    filename='./logs/client_detailed.log'
)

def initiate_transaction(source, destination, amount,simulate_crash=False):
    """
    Client transaction initiator with detailed logging.
    Initiates a transaction request to the coordinator.
    """
    coordinator_url = "http://localhost:8001"
    
    
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
    """
    Simulate a coordinator crash by calling the crash endpoint.
    """
    coordinator_url = "http://localhost:8001"
    try:
        coordinator = xmlrpc.client.ServerProxy(coordinator_url)
        print("Client: Simulating coordinator crash...")
        logging.info("Simulating coordinator crash.")
        coordinator.simulate_coordinator_crash()
    except Exception as e:
        print(f"Client: Coordinator crash simulation complete: {e}")
        logging.info(f"Coordinator crash simulation completed: {e}")

def recover_coordinator(recover):
    """
    Call the coordinator to recover from a crash.
    """
    coordinator_url = "http://localhost:8001"
    try:
        coordinator = xmlrpc.client.ServerProxy(coordinator_url)
        print("Client: Recovering coordinator...")
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

def start_participant_node_with_balance(port, initial_balance, scenario=None):
    """
    Start a participant node with an initial balance and optional crash scenario.
    """
    with xmlrpc.client.ServerProxy(f'http://localhost:{port}') as proxy:
        response = proxy.set_initial_balance(initial_balance)
        print(f"Participant Node {port}: {response}")
        
        if scenario:
            response = set_crash_scenario(proxy, scenario)
            print(f"Participant Node {port}: Crash scenario set to {scenario}.")

def main():
    """
    Main function to run transaction scenarios, including crashes and recovery.
    """
    print("\nStarting participant nodes with initial balances.")
    
    # Start participant nodes with initial balances
    start_participant_node_with_balance(8002, 200)  # Node 2
    start_participant_node_with_balance(8003, 300)  # Node 3

    print(
        """
        ************************************************************************
                     Scenario 1: Successful Transaction
        ************************************************************************
        """
    )
    initiate_transaction('A', 'B', 100)
    time.sleep(5)



 # print("\nScenario 2: Insufficient Funds")
 
    print(
    """
    ************************************************************************
                    Scenario 2: Insufficient Fund
    ************************************************************************

    """)
    start_participant_node_with_balance(8002, 90)  # Initial balance for Node 2
    start_participant_node_with_balance(8003, 50)  # Initial balance for Node 3
  

    initiate_transaction('A', 'B', 100)
    time.sleep(5)





    print(
    """
    ************************************************************************
                    Scenario 3: Crash before_response Node2
    ************************************************************************

    """)


    # Set crash scenario for Node-2 before response
    start_participant_node_with_balance(8002, 200,scenario='before_response')  # Initial balance for Node 2
    start_participant_node_with_balance(8003, 300,scenario=None)  # Initial balance for Node 3
    time.sleep(2)
    
    # Initiate the transaction after setting the crash scenario
    initiate_transaction('A', 'B', 100)


    print(
    """
    ************************************************************************
                    Scenario 4: Crash after_response Node2
    ************************************************************************

    """)
    start_participant_node_with_balance(8002, 200,scenario='after_response') 

    # Initiate another transaction after setting the crash scenario
    initiate_transaction('A', 'B', 100)
    
    time.sleep(2)  # Allow for some processing time



    print(
        """
        ************************************************************************
                     Scenario 5: Coordinator Crash During Trasaction (After broadvcasting commit)
        ************************************************************************
        """
    )
    # Initiate a transaction and simulate a crash during the process
    initiate_transaction('A', 'B', 100,simulate_crash=True)
    # Wait to simulate crash handling
    time.sleep(15)
    
    # Recover coordinator
    print("Client: Recovering coordinator...")
    recover_coordinator(recover=True)


    time.sleep(5)
    

    # print(
    #     """
    #     ************************************************************************
    #                  Scenario 5: Insufficient Funds
    #     ************************************************************************
    #     """
    # )
    # start_participant_node_with_balance(8002, 90)  # Node 2 with low balance
    # start_participant_node_with_balance(8003, 50)  # Node 3 with low balance

    # initiate_transaction('A', 'B', 100)
    # time.sleep(5)

if __name__ == "__main__":
    main()
