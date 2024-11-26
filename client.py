import xmlrpc.client
import time
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG, 
                    format='%(asctime)s - Client - %(levelname)s - %(message)s',
                    filename='client_detailed.log')

def initiate_transaction(source, destination, amount):
    """
    Client transaction initiator with detailed logging
    """
    # Coordinator's XML-RPC endpoint
    coordinator_url = "http://localhost:8001"
    
    try:
        # Create a proxy to the coordinator
        coordinator = xmlrpc.client.ServerProxy(coordinator_url)
        
        # Prepare transaction details
        transaction = {
            'source_account': source,
            'destination_account': destination,
            'amount': amount
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

def main():
    # Test different transaction scenarios

        # Scenario 1: Successful transaction
    print("\nScenario 1: Successful Transaction")
    initiate_transaction('A', 'B', 50)
    time.sleep(2)
    
    # Scenario 2: Insufficient funds
    print("\nScenario 2: Insufficient Funds")
    initiate_transaction('A', 'B', 250)
    time.sleep(2)
    
    # # Scenario 1: Successful transaction
    # print("\nScenario 1: Successful Transaction")
    # result1 = initiate_transaction("Account1", "Account2", 500)
    
    # # Scenario 2: Transaction with insufficient funds
    # print("\nScenario 2: Insufficient Funds")
    # result2 = initiate_transaction("Account1", "Account3", 10000)
    
    # # Scenario 3: Transaction to same account
    # print("\nScenario 3: Same Account Transaction")
    # result3 = initiate_transaction("Account1", "Account1", 100)
    
    # # Scenario 4: Negative amount transaction
    # print("\nScenario 4: Negative Amount Transaction")
    # result4 = initiate_transaction("Account1", "Account2", -50)
    
    # # Print overall summary
    # print("\nTransaction Scenarios Summary:")
    # print(f"Scenario 1 (Successful): {result1}")
    # print(f"Scenario 2 (Insufficient Funds): {result2}")
    # print(f"Scenario 3 (Same Account): {result3}")
    # print(f"Scenario 4 (Negative Amount): {result4}")

if __name__ == "__main__":
    main()